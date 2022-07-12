"""
Process single nucleus RNA-seq samples with Parse pipeline
"""

from enum import Enum
from os.path import basename
from typing import List, Optional, cast

from latch import small_task, workflow
from latch.functions.messages import message
from latch.types import LatchAuthor, LatchFile, LatchMetadata, LatchParameter

from wf.helper import execute_cmd, latch2local


class Command(Enum):
    """
    Commands supported by seqtk
    """

    sample = "sample"  # pylint: disable=invalid-name
    seq = "seq"  # pylint: disable=invalid-name


@small_task
def seqtk_task(  # pylint: disable=too-many-arguments
    input_file: Optional[LatchFile],
    output_file: Optional[LatchFile],
    command: Command,
    seed: int,
    fraction: Optional[float],
    num_reads: Optional[int],
    num_bases: Optional[int],
) -> Optional[LatchFile]:
    """
    Run seqtk command on input file
    """

    if input_file is None or output_file is None:
        return None

    if command == Command.sample:
        cmd = [
            "seqtk",
            command.value,
            "-s",
            str(seed),
            "-2" if num_reads else "",
            latch2local(input_file),
            str(fraction) if fraction else str(num_reads),
        ]
    elif command == Command.seq:
        cmd = [
            "seqtk",
            command.value,
            *(["-l", str(num_bases)] if num_bases else []),
            latch2local(input_file),
        ]
    else:
        raise ValueError(f"Unsupported command {command.value}")

    if cast(str, output_file.remote_path).endswith(".gz"):
        cmd.extend(
            [
                "|",
                "igzip",
            ]
        )
    cmd.extend(
        [
            ">",
            f"/root/{basename(cast(str, input_file.remote_path))}",
        ]
    )
    # filter empty strings
    cmd = [e for e in cmd if e]
    message(typ="info", data={"title": "Running seqtk", "body": f"Running {' '.join(cmd)}"})
    execute_cmd(cmd, capture_stdout=False)

    return LatchFile(
        path=f"/root/{basename(cast(str, input_file.remote_path))}",
        remote_path=output_file.remote_path,  # type: ignore
    )


metadata = LatchMetadata(
    display_name="seqtk",
    author=LatchAuthor(
        name="Tobias Fehlmann",
        email="tobias@astera.org",
        github="https://github.com/tfehlmann",
    ),
    repository="https://github.com/tfehlmann/latch-seqtk",
    license="MIT",
)

metadata.parameters["command"] = LatchParameter(
    display_name="Seqtk command",
    description="Seqtk command to run.",
    hidden=False,
)

metadata.parameters["input_file"] = LatchParameter(
    display_name="Input file",
    description="Input file passed to seqtk.",
    hidden=False,
)

metadata.parameters["input_file_r2"] = LatchParameter(
    display_name="Paired-end read R2 file",
    description="Optional paired-end read R2 file passed to seqtk.",
    hidden=False,
)

metadata.parameters["output_file"] = LatchParameter(
    display_name="Output file",
    description="Output file written by seqtk.",
    hidden=False,
    output=True,
)

metadata.parameters["output_file_r2"] = LatchParameter(
    display_name="Output file for paired-end R2",
    description="Output file for paired-end R2 written by seqtk.",
    hidden=False,
    output=True,
)

metadata.parameters["fraction"] = LatchParameter(
    display_name="Fraction",
    description="Fraction of reads to sample. Either specify the fraction or the number of reads."
    " Used in conjunction with sample command.",
    section_title="sample",
    hidden=False,
)

metadata.parameters["num_reads"] = LatchParameter(
    display_name="Number of reads",
    description="Number of reads to sample. Either specify the fraction or the number of reads."
    " Used in conjunction with sample command.",
    hidden=False,
)

metadata.parameters["seed"] = LatchParameter(
    display_name="Seed",
    description="Seed for random number generator. Used in conjunction with sample command.",
    hidden=False,
)

metadata.parameters["num_bases"] = LatchParameter(
    display_name="Number of bases",
    description="Number of bases per line. Used in conjunction with seq command.",
    section_title="seq",
    hidden=False,
)


@workflow(metadata=metadata)
def seqtk(  # pylint: disable=too-many-arguments
    command: Command,
    input_file: LatchFile,
    output_file: LatchFile,
    fraction: Optional[float] = None,
    num_reads: Optional[int] = None,
    input_file_r2: Optional[LatchFile] = None,
    output_file_r2: Optional[LatchFile] = None,
    seed: int = 42,
    num_bases: Optional[int] = None,
) -> List[Optional[LatchFile]]:
    """
    Run seqtk command on input file(s)
    """

    return [
        seqtk_task(
            command=command,
            input_file=input_file,
            output_file=output_file,
            seed=seed,
            fraction=fraction,
            num_reads=num_reads,
            num_bases=num_bases,
        ),
        seqtk_task(
            command=command,
            input_file=input_file_r2,
            output_file=output_file_r2,
            seed=seed,
            fraction=fraction,
            num_reads=num_reads,
            num_bases=num_bases,
        ),
    ]


if __name__ == "__main__":
    pass
