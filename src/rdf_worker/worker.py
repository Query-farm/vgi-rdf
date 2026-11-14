"""Launch the RDF SQL worker over stdio, local sockets or HTTP."""

import sys
from typing import Annotated

from vgi import Worker
from vgi.catalog.attach_option import AttachOption

from .catalog import CATALOG, RdfCatalog
from .config import validate_listener


class RdfWorker(Worker):
    """Native SQL catalog backed by a persistent Oxigraph RDF dataset."""

    catalog = CATALOG
    catalog_interface = RdfCatalog

    class AttachOptions:
        """Configuration supplied with SQL ATTACH."""

        database: Annotated[
            str, AttachOption(desc="Persistent RDF database directory; empty creates a temporary database.")
        ] = ""
        read_only: Annotated[bool, AttachOption(desc="Reject every data or schema mutation on this attachment.")] = (
            False
        )


def main() -> None:
    """Parse transport/logging flags and serve the VGI Arrow protocol."""
    validate_listener(sys.argv[1:])
    RdfWorker.main()


if __name__ == "__main__":
    main()
