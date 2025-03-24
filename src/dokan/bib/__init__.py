### This script requires
###         - process_name as command line argument
###         - references.py: big references dictionary
###                          which should be filled with all the needed ones
### This script generates
###         - references.bib: inpspire bibitems
###         - references.tex: list of citations
###
###         usage: python nnlojet-references.py process_name
###
### References are divided into:
###     - original calculation
###     - antenna subtraction
###     - amplitudes
###     - external_tools
###
###
###

from pathlib import Path

from .._types import GenericPath
from ._references import references

### common reference blocks

FF_antennae = [
    "Gehrmann-DeRidder:2004ttg",
    "Gehrmann-DeRidder:2005alt",
    "Gehrmann-DeRidder:2005svg",
    "Gehrmann-DeRidder:2005btv",
    "Gehrmann-DeRidder:2007foh",
]
IF_antennae = ["Daleo:2006xa", "Daleo:2009yj"]
II_antennae = ["Daleo:2006xa", "Boughezal:2010mc", "Gehrmann:2011wi", "Gehrmann-DeRidder:2012too"]
IF_II_antennae = list(
    dict.fromkeys(IF_antennae + II_antennae)
)  # remove duplicate NLO antenna reference

procs = {
    "eeJJ": {
        "ORIGINAL CALCULATIONS": [],
        "ANTENNA SUBTRACTION": FF_antennae + [],
        "AMPLITUDES": [],
        "EXTERNAL TOOLS": ["Buckley:2014ana"],
    },
    "eeJJJ": {
        "ORIGINAL CALCULATIONS": ["Gehrmann:2017xfb"],
        "ANTENNA SUBTRACTION": FF_antennae + [],
        "AMPLITUDES": [
            "Hagiwara:1988pp",
            "Berends:1988yn",
            "Bern:1997sc",
            "Garland:2001tf",
            "Garland:2002ak",
            "Gehrmann:2002zr",
            "Gehrmann:2011ab",
        ],
        "EXTERNAL TOOLS": ["Buckley:2014ana"],
    },
    "epLJ": {
        "ORIGINAL CALCULATIONS": [],
        "ANTENNA SUBTRACTION": IF_antennae + ["Gehrmann-DeRidder:2005btv"],
        "AMPLITUDES": [],
        "EXTERNAL TOOLS": ["Buckley:2014ana"],
    },
    "epLJJ": {
        "ORIGINAL CALCULATIONS": ["Currie:2017tpe"],
        "ANTENNA SUBTRACTION": FF_antennae + IF_antennae + ["Currie:2013vh"],
        "AMPLITUDES": [],
        "EXTERNAL TOOLS": ["Buckley:2014ana"],
    },
    "epNJ": {
        "ORIGINAL CALCULATIONS": [],
        "ANTENNA SUBTRACTION": IF_antennae + ["Gehrmann-DeRidder:2005btv"],
        "AMPLITUDES": [],
        "EXTERNAL TOOLS": ["Buckley:2014ana"],
    },
    "epNJJ": {
        "ORIGINAL CALCULATIONS": ["Niehues:2018was"],
        "ANTENNA SUBTRACTION": FF_antennae + IF_antennae + ["Currie:2013vh"],
        "AMPLITUDES": [],
        "EXTERNAL TOOLS": ["Buckley:2014ana"],
    },
    "epNbJ": {
        "ORIGINAL CALCULATIONS": [],
        "ANTENNA SUBTRACTION": FF_antennae + IF_antennae + ["Currie:2013vh"],
        "AMPLITUDES": [],
        "EXTERNAL TOOLS": ["Buckley:2014ana"],
    },
    "epNbJJ": {
        "ORIGINAL CALCULATIONS": ["Niehues:2018was"],
        "ANTENNA SUBTRACTION": FF_antennae + IF_antennae + ["Currie:2013vh"],
        "AMPLITUDES": [],
        "EXTERNAL TOOLS": ["Buckley:2014ana"],
    },
    "Z": {
        "ORIGINAL CALCULATIONS": ["Gehrmann-DeRidder:2023urf"],
        "ANTENNA SUBTRACTION": II_antennae + ["Gehrmann-DeRidder:2005btv"],
        "AMPLITUDES": [],
        "EXTERNAL TOOLS": ["Buckley:2014ana"],
    },
    "ZJ": {
        "ORIGINAL CALCULATIONS": [
            "Gehrmann-DeRidder:2015wbt",
            "Gehrmann-DeRidder:2016jns",
            "Gehrmann-DeRidder:2016cdi",
            "Gauld:2017tww",
            "Gehrmann-DeRidder:2019avi",
            "Gauld:2021pkr",
            "Gehrmann-DeRidder:2023urf",
        ],
        "ANTENNA SUBTRACTION": FF_antennae + IF_II_antennae + ["Currie:2013vh"],
        "AMPLITUDES": [
            "Hagiwara:1988pp",
            "Berends:1988yn",
            "Bern:1997sc",
            "Garland:2001tf",
            "Garland:2002ak",
            "Gehrmann:2002zr",
            "Gehrmann:2011ab",
        ],
        "EXTERNAL TOOLS": ["Buckley:2014ana"],
    },
    "W": {
        "ORIGINAL CALCULATIONS": [],
        "ANTENNA SUBTRACTION": II_antennae + ["Gehrmann-DeRidder:2005btv"],
        "AMPLITUDES": [],
        "EXTERNAL TOOLS": ["Buckley:2014ana"],
    },
    "WJ": {
        "ORIGINAL CALCULATIONS": ["Gehrmann-DeRidder:2017mvr", "Gehrmann-DeRidder:2019avi"],
        "ANTENNA SUBTRACTION": FF_antennae + IF_II_antennae + ["Currie:2013vh"],
        "AMPLITUDES": [
            "Hagiwara:1988pp",
            "Berends:1988yn",
            "Bern:1997sc",
            "Garland:2001tf",
            "Garland:2002ak",
            "Gehrmann:2002zr",
            "Gehrmann:2011ab",
        ],
        "EXTERNAL TOOLS": ["Buckley:2014ana"],
    },
    "H": {
        "ORIGINAL CALCULATIONS": [],
        "ANTENNA SUBTRACTION": II_antennae + ["Gehrmann-DeRidder:2005btv"],
        "AMPLITUDES": [],
        "EXTERNAL TOOLS": ["Buckley:2014ana"],
    },
    "HJ": {
        "ORIGINAL CALCULATIONS": ["Chen:2014gva"],
        "ANTENNA SUBTRACTION": FF_antennae + IF_II_antennae + ["Currie:2013vh"],
        "AMPLITUDES": [
            "DelDuca:2004wt",
            "Dixon:2009uk",
            "Badger:2009hw",
            "Badger:2009vh",
            "Gehrmann:2011aa",
        ],
        "EXTERNAL TOOLS": ["Buckley:2014ana"],
    },
    "H2J": {
        "ORIGINAL CALCULATIONS": ["Chen:2016zka"],
        "ANTENNA SUBTRACTION": FF_antennae + IF_II_antennae + ["Currie:2013vh"],
        "AMPLITUDES": [
            "DelDuca:2004wt",
            "Dixon:2009uk",
            "Badger:2009hw",
            "Badger:2009vh",
            "Gehrmann:2011aa",
        ],
        "EXTERNAL TOOLS": ["Buckley:2014ana"],
    },
    "H3J": {
        "ORIGINAL CALCULATIONS": ["Chen:2021ibm"],
        "ANTENNA SUBTRACTION": FF_antennae + IF_II_antennae + ["Currie:2013vh"],
        "AMPLITUDES": [
            "DelDuca:2004wt",
            "Dixon:2009uk",
            "Badger:2009hw",
            "Badger:2009vh",
            "Gehrmann:2011aa",
        ],
        "EXTERNAL TOOLS": ["Buckley:2014ana"],
    },
    "H4J": {
        "ORIGINAL CALCULATIONS": ["Chen:2019wxf"],
        "ANTENNA SUBTRACTION": FF_antennae + IF_II_antennae + ["Currie:2013vh"],
        "AMPLITUDES": [
            "DelDuca:2004wt",
            "Dixon:2009uk",
            "Badger:2009hw",
            "Badger:2009vh",
            "Gehrmann:2011aa",
        ],
        "EXTERNAL TOOLS": ["Buckley:2014ana"],
    },
    "GJ": {
        "ORIGINAL CALCULATIONS": ["Chen:2019zmr"],
        "ANTENNA SUBTRACTION": FF_antennae + IF_II_antennae + ["Currie:2013vh"],
        "AMPLITUDES": ["DelDuca:1999pa", "Signer:1995np", "Anastasiou:2002zn"],
        "EXTERNAL TOOLS": ["Buckley:2014ana"],
    },
    "GG": {
        "ORIGINAL CALCULATIONS": ["Gehrmann:2020oec"],
        "ANTENNA SUBTRACTION": II_antennae + ["Currie:2013vh", "Chen:2022clm"],
        "AMPLITUDES": [],
        "EXTERNAL TOOLS": ["Buckley:2014ana"],
    },
    "JJ": {
        "ORIGINAL CALCULATIONS": [
            "Currie:2016bfm",
            "Currie:2017eqf",
            "Currie:2018xkj",
            "Gehrmann-DeRidder:2019ibf",
            "Chen:2022tpk",
        ],
        "ANTENNA SUBTRACTION": FF_antennae
        + IF_II_antennae
        + [
            "NigelGlover:2010kwr",
            "Gehrmann-DeRidder:2011jwo",
            "Gehrmann-DeRidder:2012dog",
            "Currie:2013vh",
        ],
        "AMPLITUDES": [
            "Berends:1987me",
            "Mangano:1990by",
            "Kuijf:thesis",
            "Bern:1993mq",
            "Bern:1994fz",
            "Kunszt:1994nq",
            "Signer:thesis",
            "Anastasiou:2001sv",
            "Glover:2001rd",
            "Glover:2001af",
            "Anastasiou:2000kg",
            "Anastasiou:2000ue",
            "Anastasiou:2000mv",
        ],
        "EXTERNAL TOOLS": ["Buckley:2014ana"],
    },
}

simplify_names: dict[str, str] = {
    "EPEMJJ": "eeJJ",
    "EPEM": "eeJJJ",
    "DIS1j": "epLJ",
    "DISWm1j": "epNJ",
    "DISWp1j": "epNbJ",
    "DIS": "epLJJ",
    "DISWm": "epNJJ",
    "DISWp": "epNbJJ",
    "1jet": "JJ",
    "2jet": "JJ",
    "2jetfc": "JJ",
    "ZJJ": "ZJ",
    "ZJJJ": "ZJ",
    "WJJ": "WJ",
    "WJJJ": "WJ",
    "HJJ": "HJ",
    "HJJJ": "HJ",
    "H2JJ": "H2J",
    "H2JJJ": "H2J",
    "H3JJ": "H3J",
    "H3JJJ": "H3J",
    "H4JJ": "H4J",
    "H4JJJ": "H4J",
    "GJJ": "GJ",
    "GJJJ": "GJ",
    "GGJ": "GG",
    "GGJJ": "GG",
}


map_names: dict[str, str] = {
    "Wp": "W",
    "WpJ": "WJ",
    "Wm": "W",
    "WmJ": "WJ",
    "HTO2P": "H",
    "HTO2TAU": "H",
    "HTO2PJ": "H2J",
    "HTO2TAUJ": "H2J",
    "HTO2L1P": "H",
    "HTO2L1PJ": "H3J",
    "HTO4E": "H",
    "HTO2E2MU": "H4",
    "HTO2L2N": "H4",
    "HTO4EJ": "H4J",
    "HTO2E2MUJ": "H4J",
    "HTO2L2NJ": "H4J",
}


def make_bib(proc: str, destination: GenericPath) -> tuple[Path, Path]:
    dest_path: Path = Path(destination)
    if not dest_path.exists():
        dest_path.mkdir(parents=True)
    if not dest_path.is_dir():
        raise ValueError(f"make_bib: {destination} is not a folder")

    original_proc: str = proc

    # > make everything case insensitive
    upper_proc: str = proc.upper()
    upper_simplify_names: dict[str, str] = {k.upper(): v.upper() for k, v in simplify_names.items()}
    upper_map_names: dict[str, str] = {k.upper(): v.upper() for k, v in map_names.items()}
    if upper_proc in upper_map_names:
        upper_proc = upper_map_names[upper_proc]
    if upper_proc in upper_simplify_names:
        upper_proc = upper_simplify_names[upper_proc]

    upper_procs = {k.upper(): v for k, v in procs.items()}

    if upper_proc not in upper_procs:
        raise ValueError(f"make_bib: cannot recognise process {original_proc} [{upper_proc}]")

    entry = upper_procs[upper_proc]

    # > inject the NNLOJET code reference at the front
    entry = {"NNLOJET": ["NNLOJET:2025xyz"]} | entry

    bibout: Path = dest_path / f"NNLOJET_references_{original_proc}.bib"
    bibtex: Path = dest_path / f"NNLOJET_references_{original_proc}.tex"

    with open(bibout, "w") as bib:
        for type in entry.keys():
            for ref in entry[type]:
                bib.write(references[ref])

    with open(bibtex, "w") as bib:
        bib.write("%%% Please cite:\n")
        for type in entry.keys():
            if not entry[type]:
                continue
            bib.write("\n%%% " + type + "\n")
            bib.write("\\cite{" + ",".join(entry[type]) + "}\n")

    return bibout, bibtex
