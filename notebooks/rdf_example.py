"""Disposable RDF storage, one transaction session and result-only figures."""

import os
from pathlib import Path
from tempfile import TemporaryDirectory

import matplotlib.pyplot as plt
import numpy as np
from vgi_notebooks import finish


class Store:
    """Own a disposable store and restore inherited RDF settings at cleanup."""

    def __init__(self):
        self.directory = TemporaryDirectory(prefix="vgi-rdf-example-")
        self.saved = {k: v for k, v in os.environ.items() if k.startswith("VGI_RDF_")}
        for key in self.saved:
            os.environ.pop(key)
        os.environ["VGI_RDF_ROOT"] = self.directory.name
        self.path = str(Path(self.directory.name) / "store")

    def close(self):
        for key in list(os.environ):
            if key.startswith("VGI_RDF_"):
                os.environ.pop(key)
        os.environ.update(self.saved)
        self.directory.cleanup()


class TransactionSession:
    """Keep JupySQL cells on the same native session for explicit BEGIN/COMMIT."""

    def __init__(self, connection):
        self.connection = connection

    def cursor(self):
        return self.connection

    def commit(self):
        self.connection.commit()

    def close(self):
        self.connection.close()


def configure(value, name):
    """Validate controls before touching an RDF store."""
    allowed = {"MAX_DEPTH": {1, 2, 3}, "TRANSFER_TO": {2, 3}}
    if type(value) is not int or value not in allowed[name]:
        raise ValueError(f"{name} must be one of {sorted(allowed[name])}.")


def plot_lineage(path, MAX_DEPTH):
    """Present materialized worker results without recreating graph analysis."""
    fig, ax = plt.subplots(figsize=(12, 5.5))
    ax.set_xlim(-0.55, 3.55)
    ax.set_ylim(-0.8, 0.8)
    ax.axis("off")
    for row in path.itertuples():
        color = "#b66b4d" if row.depth == 0 else ("#2f8194" if row.reached else "#d9dfdd")
        ax.scatter(row.depth, 0, s=2500, color=color, zorder=3, edgecolor="white", linewidth=2)
        ax.text(row.depth, 0.43, row.label, ha="center", fontsize=12, weight="bold")
        ax.text(row.depth, -0.45, "start" if row.depth == 0 else f"depth {row.depth}", ha="center", fontsize=11)
        if row.depth:
            ax.annotate(
                "",
                xy=(row.depth - 0.19, 0),
                xytext=(row.depth - 1 + 0.19, 0),
                arrowprops={"arrowstyle": "->", "color": "#75827c", "lw": 2},
                zorder=2,
            )
    finish(
        fig,
        "A depth limit bounds discovery, not the underlying path",
        f"Line item 1003 · orange start · teal reached · gray beyond {MAX_DEPTH} edges",
        "Synthetic six-item ledger · actual minimum-depth traversal through RDF v0.2.0\nGray nodes come from the known SQL lineage; they are outside the discovery bound.",
        name="01-lineage",
    )


def plot_revenue(revenue):
    """Present materialized worker results without recreating graph analysis."""
    fig, ax = plt.subplots(figsize=(11, 6))
    x = np.arange(len(revenue))
    width = 0.34
    for offset, column, label, color in [
        (-width / 2, "sql_usd", "SQL joins", "#2f8194"),
        (width / 2, "rdf_usd", "RDF path + typed rows", "#b66b4d"),
    ]:
        bars = ax.bar(x + offset, revenue[column].astype(float), width, label=label, color=color)
        ax.bar_label(bars, fmt="%.2f", padding=4, fontsize=10)
    ax.set_xticks(x, revenue.region)
    ax.set_ylabel("Net line-item amount · USD")
    ax.set_xlabel("Region")
    fig.legend(*ax.get_legend_handles_labels(), loc="upper left", bbox_to_anchor=(0.07, 0.82), ncol=2, fontsize=10)
    ax.set_ylim(0, float(revenue.sql_usd.max()) * 1.30)
    finish(
        fig,
        "Two query models preserve the same ledger totals",
        "Three foreign-key steps · decimal USD amounts · negative credits retained",
        "Synthetic six-item ledger · actual SQL joins and SPARQL paths in one RDF v0.2.0 store\nAgreement checks the same stored data through two interfaces; it is not independent source verification.",
        name="02-revenue",
    )


def plot_ownership(before, after, target):
    """Keep all owners and the nullable unassigned category in both snapshots."""
    fig, axes = plt.subplots(1, 2, figsize=(11.5, 6), sharey=True)
    labels = ["Aster", "Briar", "Cedar", "Unassigned"]
    for ax, frame, label in zip(axes, [before, after], ["Before transfer", "Committed and reopened"], strict=True):
        ordered = frame.set_index("owner").loc[labels]
        colors = ["#2F8194" if n != "Unassigned" else "#AAB9B0" for n in labels]
        bars = ax.barh(labels, ordered.task_count.astype(int), color=colors, height=0.58)
        ax.bar_label(bars, padding=7, fmt="%d", fontsize=10)
        ax.set_xlim(0, 3)
        ax.set_xticks([0, 1, 2, 3])
        ax.set_xlabel("Tasks · count")
        ax.set_title(label, loc="left", fontsize=12, weight="bold", pad=15)
        ax.grid(axis="x")
        ax.set_axisbelow(True)
        ax.tick_params(axis="y", length=0)
    axes[0].invert_yaxis()
    finish(
        fig,
        "One foreign-key edit moves one relationship",
        f"Task 102 moves from Aster to {target} · all four tasks remain, including the NULL owner",
        "Synthetic owners and tasks · SQL ownership agrees with stored RDF edges and neighbor lookup\n"
        "RDF v0.2.0 · same durable directory reopened read-only; deleted on notebook cleanup.",
        name="03-ownership",
    )
