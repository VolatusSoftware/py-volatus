"""
Provides CalcsModule functionality. Automatically registers the CalcsModule implementation
with the Volatus framework so that a task type of "PyCalcs" can be launched automatically
when specified in a vjson file.

Also provides stateful calculation helpers such as RunningAvg
"""
from .telemetry import ChannelGroup, ChannelValue
from .volatus import Module, register_module

from collections.abc import Callable
from types import CodeType
import asyncio
import re

type CalcMethod = Callable[[dict[str, float]], float]
"""Represents a method that calculates a single value from a dictionary of input values."""

class RunningAvg:
    def __init__(self, count: int):
        self._count = count
        self._vals: list[float] = []
        self._avg: float = 0.0

    def add(self, val: float) -> float:
        val = val / self._count
        self._vals.append(val)
        self._avg += val

        if len(self._vals) > self._count:
            self._avg -= self._vals.pop(0)

        return self._avg

class EmaFilter:
    def __init__(self, alpha: float):
        self._a = alpha
        self._b = 1 - alpha
        self._val = 0

    def calc(self, val: float) -> float:
        self._val = self._a * val + self._b * self._val
        return self._val

def avg(*args) -> float:
    return sum(args) / len(args)

class Calc:
    _private_token = object()

    def __init__(self, token, inputs: list[ChannelValue], output: ChannelValue):
        if token is not Calc._private_token:
            raise TypeError("Calc cannot be instantiated directly. Use a Calc.from_###() method instead.")

        self.inputs = {c.name: c for c in inputs}
        self.output = output
        self.input_vals = {name: 0.0 for name in inputs}
        self.method: CalcMethod = None
        self.calc: CodeType = None
        self.fn: dict[str, Callable] = {}

    @staticmethod
    def from_expression(inputs: list[ChannelValue], output: ChannelValue, expression: str) -> "Calc":
        calc = Calc(Calc._private_token, inputs, output)
        calc._set_expr(expression, output.name)

        return calc

    @staticmethod
    def from_method(inputs: list[ChannelValue], output: ChannelValue, method: CalcMethod) -> "Calc":
        calc = Calc(Calc._private_token, inputs, output)
        calc.method = method

        return calc

    def _create_fn(self, fn_name: str, args: list[str]) -> str | None:
        match fn_name:
            case 'run_avg':
                avg = RunningAvg(int(args[1]))
                f_num = len(self.fn)
                name = f"f{f_num}"
                self.fn[name] = avg.add
                return f"self.fn['{name}']({args[0]})"

            case 'ema':
                ema = EmaFilter(float(args[1]))
                f_num = len(self.fn)
                name = f"f{f_num}"
                self.fn[name] = ema.calc
                return f"self.fn['{name}']({args[0]})"

            case _:
                # Not a stateful fn, pass back None so that original isn't modified
                return None

    def _build_stateful_fns(self, expression: str) -> str:
        """
        Identifies function calls in the expression, creating the necessary
        handlers and returning the updated expression.
        Expects that input channel names have already been updated and only
        accomodates Calcs module stateful functions."""

        eval_str = ""
        pattern = r"(\w+)\(([^\(]*)\)" # group indices: 0: Whole, 1: fn name, 2: args
        last_end = 0

        for m in re.finditer(pattern, expression):
            fn = m.group(1)
            args = m.group(2).split(',')
            print(f"{m.start()}-{m.end()} {fn}({args})")
            eval_str = eval_str + expression[last_end:m.start()]
            last_end = m.end()

            updated = self._create_fn(fn, args)
            if not updated:
                eval_str = eval_str + m.group(0)
            else:
                eval_str = eval_str + updated

        eval_str = eval_str + expression[last_end:]
        print(f"'{expression}' => '{eval_str}'")
        return eval_str


    def _set_expr(self, expression: str, output_name: str):
        expression = self._build_stateful_fns(expression)
        try:
            self.calc = compile(expression, f"<Calc:{output_name}>", "eval")
            self.method = self._do_expr
        except Exception as e:
            print(f"{e}")

    def _do_expr(self, inputs: dict[str, float]) -> float: #ignore unused, is available as local for the eval()
        return eval(self.calc)

    def do_calc(self):
        for name, channel in self.inputs.items():
            self.input_vals[name] = channel.value

        val = self.method(self.input_vals)
        self.output.value = val

class CalcConfig:
    def __init__(self, output: str, expression: str = None, method: CalcMethod = None, inputs: list[str] = []):
        self.output = output
        self.expression = expression
        self.method = method
        self.inputs = inputs

    @staticmethod
    def as_expression(output: str, expression: str) -> "CalcConfig":
        return CalcConfig(
            output=output,
            expression=expression,
        )

    @staticmethod
    def as_method(output: str, method: CalcMethod, inputs: list[str] = []) -> "CalcConfig":
        return CalcConfig(
            output=output,
            method=method,
            inputs=inputs
        )
    
class CalcsModule(Module):
    input_channels: dict[str, ChannelValue] = {} # All channels used for 
    input_groups: dict[str, ChannelGroup] = {} # Doesn't need to be manually updated or published
    output_groups: dict[str, ChannelGroup] = {} # Groups that get published each iteration

    calcs: list[Calc] = []

    period: float = 0.1 # Delay between each calculations iteration

    @staticmethod
    def module_type():
        return "PyCalcs"

    def module_init(self):
        cfg_period = self.task_config.lookupChildByName("period_ms")
        if cfg_period:
            self.period = cfg_period.value()


        self._reg_q: asyncio.Queue[CalcConfig] = asyncio.Queue()

    async def module_loop(self):
        await self._load_calcs()

        while True:
            while not self._reg_q.empty():
                cfg = self._reg_q.get_nowait()
                if cfg.expression:
                    await self.add_calc_expression(cfg.output, cfg.expression)
                elif cfg.method:
                    await self.add_calc_method(cfg.output, cfg.method, cfg.inputs)

            await asyncio.sleep(self.period)
            for calc in self.calcs:
                calc.do_calc()

            for _, g in self.output_groups.items():
                self.v.publish(g)
        
    async def _load_calcs(self):
        for _, group in self.task_config.groups.items():
            for ch_name, channel in group.channels.items():
                expression = channel.lookupChildByName("Expression")
                if expression:
                    await self.add_calc_expression(ch_name, expression.value())

    async def add_calc_expression(self, output: str, expression: str):
        input_chans: list[ChannelValue] = []
        output_chan: ChannelValue = None

        inputs: list[str] = []
        eval_str = ""

        pattern = r"\[([^\]]+)\]"
        last_end = 0
        for m in re.finditer(pattern, expression):
            input = m.group(1)
            print(f"{m.start()}-{m.end()} {input}")
            eval_str = eval_str + expression[last_end:m.start()] + f"inputs[\"{input}\"]"
            inputs.append(input)
            last_end = m.end()

        eval_str = eval_str + expression[last_end:]
        print(f"'{expression}' => '{eval_str}'")

        for chan_name in inputs:
            if chan_name not in self.input_channels:
                group_name = self.v.config.group_name_for_channel(chan_name)
                if group_name:
                    if group_name not in self.input_groups:
                        self.input_groups[group_name] = (await self.v.subscribe(group_name))[0]

                self.input_channels[chan_name] = self.input_groups[group_name].chanByName(chan_name)

            input_chans.append(self.input_channels[chan_name])

        group_name = self.v.config.group_name_for_channel(output)
        if group_name:
            if group_name not in self.output_groups:
                # output groups are always for publish
                self.output_groups[group_name] = await self.v.registerForPublish(group_name)
        output_chan = self.output_groups[group_name].chanByName(output)

        self.calcs.append(Calc.from_expression(input_chans, output_chan, eval_str))

    async def add_calc_method(self, output: str, method: CalcMethod, inputs: list[str] = []):
        input_chans: list[ChannelValue] = []
        output_chan: ChannelValue = None

        for chan_name in inputs:
            if chan_name not in self.input_channels:
                group_name = self.v.config.group_name_for_channel(chan_name)
                if group_name:
                    if group_name not in self.input_groups:
                        self.input_groups[group_name] = await self.create_group(group_name)

                self.input_channels[chan_name] = self.input_groups[group_name].chanByName(chan_name)

            input_chans.append(self.input_channels[chan_name])

        group_name = self.v.config.group_name_for_channel(output)
        if group_name:
            if group_name not in self.output_groups:
                # output groups are always for publish
                self.output_groups[group_name] = await self.v.registerForPublish(group_name)
        output_chan = self.output_groups[group_name].chanByName(output)

        self.calcs.append(Calc.from_method(input_chans, output_chan, method))

register_module(CalcsModule)