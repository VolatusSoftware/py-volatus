from glob import glob
from os.path import dirname
from pathlib import Path
import subprocess

def main():
    print("\n  == gen-proto ==")
    print("Starting Python ProtoBuf message generation for the Volatus framework.")
    print(" * This expects lv-vecto cloned as a sibling of lv-volatus.")
    print(" * Ensure protoc is available on the path and is available in the shell.")

    root = Path(__file__).parent.parent
    print(f"\nRoot: {root}")

    protoRoot = root.joinpath("proto-volatus").joinpath("proto")
    print(f"Proto Root: {protoRoot}")

    outDir = root.joinpath("src").joinpath("proto")
    print(f"Out dir: {outDir}")

    protoDefs = glob(str(protoRoot) + "\\*.proto")
    print("\n  == Generating messages ==")

    if not outDir.exists():
        print(f"Creating output directory: {outDir}")
        outDir.mkdir(parents=True)

    for proto in protoDefs:
        print("Building proto message for " + proto + " ... ", end="")
        subprocess.run([
            "protoc",
            "--proto_path=" + str(protoRoot),
            "--pyi_out=" + str(outDir),
            "--python_out=" + str(outDir),
            proto
            ])
        print (" Done")


if __name__ == "__main__":
    main()
else:
    print("This file must be run directly.")
