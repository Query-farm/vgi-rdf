"""Presentation, isolated worker environment, and one native SQL session."""

import os
from pathlib import Path
from tempfile import TemporaryDirectory

import matplotlib.pyplot as plt
import seaborn as sns
from IPython.display import Markdown, display


class WorkerSandbox:
    """Give this example disposable storage, restoring inherited settings at cleanup."""

    def __init__(self, prefix):
        self.directory = TemporaryDirectory(prefix="vgi-notebook-data-")
        self.saved = {k: v for k, v in os.environ.items() if k.startswith(prefix)}
        for k in self.saved:
            os.environ.pop(k)
        self.prefix = prefix
        if prefix == "VGI_RDF_":
            os.environ["VGI_RDF_ROOT"] = self.directory.name
        else:
            os.environ["RAINCLOUD_HOME"] = str(Path(self.directory.name) / "build")
            os.environ["RAINCLOUD_CACHE"] = str(Path(self.directory.name) / "cache")

    def close(self):
        for k in list(os.environ):
            if k.startswith(self.prefix):
                os.environ.pop(k)
        os.environ.update(self.saved)
        self.directory.cleanup()


class HaybarnSession:
    """Keep JupySQL statements on one native session for variables and transactions."""

    def __init__(self, connection):
        self.connection = connection

    def cursor(self):
        return self.connection

    def commit(self):
        self.connection.commit()

    def close(self):
        self.connection.close()


def theme():
    sns.set_theme(style="whitegrid", context="notebook", font="DejaVu Sans")
    plt.rcParams.update(
        {"figure.dpi": 130, "text.parse_math": False, "axes.spines.top": False, "axes.spines.right": False}
    )


def prose(text):
    display(Markdown(text))


def finish(fig, name, title, subtitle, source):
    fig.suptitle(title, x=0.07, y=0.98, ha="left", fontsize=17, weight="bold", color="#253d31")
    fig.text(0.07, 0.89, subtitle, fontsize=11, color="#52675a")
    fig.text(0.07, 0.025, source, fontsize=9, color="#52675a")
    fig.tight_layout(rect=(0.025, 0.10, 0.98, 0.85))
    folder = Path("validation/charts")
    folder.mkdir(parents=True, exist_ok=True)
    fig.savefig(folder / (name + ".png"), dpi=150)
    plt.show()
