import logging
import os
import pathlib
from typing import Dict, Iterable, List, Optional, Set, Tuple, TypeVar, Union

import click
import progressbar
import requests
import ruamel.yaml.util
from datahub.ingestion.graph.client import (DatahubClientConfig, DataHubGraph,
                                            get_default_graph)
from datahub.ingestion.source.metadata.business_glossary import (
    BusinessGlossaryConfig, BusinessGlossaryFileSource, DefaultConfig,
    GlossaryNodeConfig, GlossaryNodeInterface, GlossaryTermConfig,
    KnowledgeCard, Owners, materialize_all_node_urns, populate_path_vs_id)
from datahub.metadata.schema_classes import (AspectBag, DomainsClass,
                                             GlossaryNodeInfoClass,
                                             GlossaryRelatedTermsClass,
                                             GlossaryTermInfoClass,
                                             InstitutionalMemoryClass,
                                             OwnershipClass)
from datahub.utilities.urns.urn import guess_entity_type
from ruamel.yaml import YAML

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

ParentUrn = str
TEST_MODE = os.getenv("TEST_MODE", "false").lower() == "true"

_DELETED_NODE_TOMBSTONE = dict()

_GlossaryElement = TypeVar("_GlossaryElement", GlossaryNodeConfig, GlossaryTermConfig)


def get_graph() -> DataHubGraph:
    if TEST_MODE:
        return get_default_graph()
    else:
        DATAHUB_SERVER = os.environ["DATAHUB_GMS_HOST"]
        DATAHUB_TOKEN: Optional[str] = os.getenv("DATAHUB_GMS_TOKEN")

        return DataHubGraph(
            DatahubClientConfig(server=DATAHUB_SERVER, token=DATAHUB_TOKEN)
        )


def _get_id_from_urn(urn: str) -> str:
    return urn.split(":")[-1]


def _datahub_ownership_to_owners(
    ownership: Optional[OwnershipClass],
) -> Optional[Owners]:
    if ownership is None:
        return None

    owner_urns = [owner.owner for owner in ownership.owners]
    owner_type = "DEVELOPER"
    owner_type_urn = None
    for owners in ownership.owners:
        if owners.typeUrn:
            # Current glossary file format does not support type per user or group
            # So we just take the first one for now
            # More accurate representation would require changing the glossary file format
            owner_type_urn = owners.typeUrn
            owner_type = owners.type
            break

    return Owners(
        users=[
            _get_id_from_urn(urn)
            for urn in owner_urns
            if guess_entity_type(urn) == "corpuser"
        ]
        or None,
        groups=[
            _get_id_from_urn(urn)
            for urn in owner_urns
            if guess_entity_type(urn) == "corpGroup"
        ]
        or None,
        type=owner_type,
        typeUrn=owner_type_urn,
    )


def _datahub_domain_to_str(domain: Optional[DomainsClass]) -> Optional[str]:
    if domain is None or not domain.domains:
        return None

    if len(domain.domains) > 1:
        logger.warning(f"Found multiple domains for {domain} - using first one")
        return None

    # TODO: Need to look up the domain name from the urn.
    return domain.domains[0]


def _datahub_institutional_memory_to_knowledge_links(
    institutionalMemory: Optional[InstitutionalMemoryClass],
) -> Optional[List[KnowledgeCard]]:
    if institutionalMemory is None:
        return None

    return [
        KnowledgeCard(
            url=element.url,
            label=element.description,
        )
        for element in institutionalMemory.elements
    ]


def _glossary_node_from_datahub(
    urn: str, aspects: AspectBag
) -> Tuple[Optional[GlossaryTermConfig], Optional[ParentUrn]]:
    try:
        info_aspect: GlossaryNodeInfoClass = aspects["glossaryNodeInfo"]
    except KeyError:
        logger.error(f"Skipping URN {urn} due to missing 'glossaryNodeInfo' aspect")
        return None, None

    owners = aspects.get("ownership")
    institutionalMemory = aspects.get("institutionalMemory")

    node = GlossaryNodeConfig(
        id=urn,
        name=info_aspect.name or _get_id_from_urn(urn),
        description=info_aspect.definition,
        owners=_datahub_ownership_to_owners(owners),
        knowledge_links=_datahub_institutional_memory_to_knowledge_links(
            institutionalMemory
        ),
        # These are populated later.
        terms=[],
        nodes=[],
    )
    node._urn = urn
    parent_urn = info_aspect.parentNode

    return node, parent_urn


def _glossary_term_from_datahub(
    urn: str, aspects: AspectBag
) -> Tuple[Optional[GlossaryTermConfig], Optional[ParentUrn]]:
    try:
        info_aspect: GlossaryTermInfoClass = aspects["glossaryTermInfo"]
    except KeyError:
        logger.error(f"Skipping URN {urn} due to missing 'glossaryTermInfo' aspect")
        return None, None

    related_terms: GlossaryRelatedTermsClass = aspects.get(
        "glossaryRelatedTerms", GlossaryRelatedTermsClass()
    )
    owners = aspects.get("ownership")
    domain = aspects.get("domains")
    institutionalMemory = aspects.get("institutionalMemory")

    term = GlossaryTermConfig(
        id=urn,
        name=info_aspect.name or _get_id_from_urn(urn),
        description=info_aspect.definition,
        term_source=info_aspect.termSource,
        source_ref=info_aspect.sourceRef,
        source_url=info_aspect.sourceUrl,
        owners=_datahub_ownership_to_owners(owners),
        custom_properties=info_aspect.customProperties or None,
        knowledge_links=_datahub_institutional_memory_to_knowledge_links(
            institutionalMemory
        ),
        domain=_datahub_domain_to_str(domain),
        # Where possible, these are converted into biz glossary paths later.
        inherits=related_terms.isRelatedTerms,
        contains=related_terms.hasRelatedTerms,
        values=related_terms.values,
        related_terms=related_terms.relatedTerms,
    )

    term._urn = urn
    parent_urn = info_aspect.parentNode

    return term, parent_urn


def fetch_datahub_glossary():
    graph = get_graph()

    logger.info("Get all the urns in the glossary.")
    urns = list(graph.get_urns_by_filter(entity_types=["glossaryTerm", "glossaryNode"]))
    logger.info(f"Got {len(urns)} urns")

    logger.info("Hydrate them into entities.")
    entities = {}
    for urn in progressbar.progressbar(urns):
        try:
            entities[urn] = graph.get_entity_semityped(urn)
        except requests.exceptions.HTTPError as e:
            logger.error(f"Error for urn {urn}: {e}")

    # Map these into pydantic models defined in the biz glossary source.
    # 1. Map each AspectBag -> pydantic model.
    # 2. Construct the hierarchy of pydantic models using the lookup table.

    logger.info("Parse glossary nodes.")
    raw_nodes = {}
    for urn in progressbar.progressbar(urns):
        if guess_entity_type(urn) != "glossaryNode" or urn not in entities:
            continue
        raw_nodes[urn] = _glossary_node_from_datahub(urn, entities[urn])

    logger.info("Construct the hierarchy of nodes.")
    top_level_nodes: List[GlossaryNodeConfig] = []
    for node, parent_urn in raw_nodes.values():
        if node is None:
            continue
        if parent_urn is None:
            top_level_nodes.append(node)
        else:
            parent_node, _ = raw_nodes[parent_urn]
            parent_node.nodes = parent_node.nodes or []
            parent_node.nodes.append(node)

    logger.info("Parse glossary terms.")
    raw_terms = {}
    for urn in progressbar.progressbar(urns):
        if guess_entity_type(urn) != "glossaryTerm" or urn not in entities:
            continue
        raw_terms[urn] = _glossary_term_from_datahub(urn, entities[urn])

    logger.info("Construct the hierarchy of terms.")
    top_level_terms: List[GlossaryTermConfig] = []
    for term, parent_urn in raw_terms.values():
        if term is None:
            continue
        if parent_urn is None:
            top_level_terms.append(term)
        elif parent_urn not in raw_nodes:
            logger.error(f"Unable to find parent urn: {parent_urn} for term {term.id}")
            top_level_terms.append(term)
        else:
            parent_node, _ = raw_nodes[parent_urn]
            parent_node.terms = parent_node.terms or []
            parent_node.terms.append(term)

    return top_level_nodes, top_level_terms


def prune_latest_glossary(
    latest_glossary: BusinessGlossaryConfig, existing_glossary: BusinessGlossaryConfig
) -> None:
    # TODO: Update this logic to allow for pruning of nodes within the hierarchy as well.

    allowed_node_urns = set(node._urn for node in (existing_glossary.nodes or []))
    allowed_term_urns = set(term._urn for term in (existing_glossary.terms or []))

    latest_glossary.nodes = [
        node for node in (latest_glossary.nodes or []) if node._urn in allowed_node_urns
    ]
    latest_glossary.terms = [
        term for term in (latest_glossary.terms or []) if term._urn in allowed_term_urns
    ]


def replace_urn_refs_with_paths(
    glossary: BusinessGlossaryConfig, path_to_id_map: Dict[str, str]
) -> None:
    urn_to_path_map = {urn: path for (path, urn) in path_to_id_map.items()}

    def _simplify_urn_list(urns: Optional[List[str]]) -> Optional[List[str]]:
        if urns is None:
            return None

        return [urn_to_path_map.get(urn, urn) for urn in urns]

    def _process_child_terms(parent_node: GlossaryNodeInterface) -> None:
        for term in parent_node.terms or []:
            term.inherits = _simplify_urn_list(term.inherits)
            term.contains = _simplify_urn_list(term.contains)
            term.values = _simplify_urn_list(term.values)
            term.related_terms = _simplify_urn_list(term.related_terms)

        for node in parent_node.nodes or []:
            _process_child_terms(node)

    _process_child_terms(glossary)


def _align_glossary_elements(
    latest: List[_GlossaryElement], existing: List[_GlossaryElement]
) -> Iterable[Tuple[Optional[_GlossaryElement], Optional[_GlossaryElement]]]:
    latest_by_id = {element._urn: element for element in latest}
    latest_ids = set(latest_by_id.keys())

    # Emit all existing elements first, matching where with latest where possible.
    for existing_element in existing:
        if existing_element._urn in latest_ids:
            latest_ids.remove(existing_element._urn)
            yield latest_by_id[existing_element._urn], existing_element
        else:
            yield None, existing_element

    # Emit all new elements.
    for latest_id in latest_ids:
        yield latest_by_id[latest_id], None


def glossary_to_dict_minimize_diffs(
    latest_glossary: BusinessGlossaryConfig, existing_glossary: BusinessGlossaryConfig
) -> dict:
    def _simple_elem_to_dict(
        latest_elem: Union[BusinessGlossaryConfig, _GlossaryElement],
        existing_elem: Union[None, BusinessGlossaryConfig, _GlossaryElement],
        defaults: DefaultConfig,
        exclude: Optional[Set[str]] = None,
    ) -> dict:
        if isinstance(latest_elem, BusinessGlossaryConfig):
            return latest_elem.dict(
                exclude=exclude, exclude_defaults=True, exclude_none=True
            )
        assert not isinstance(existing_elem, BusinessGlossaryConfig)

        # Exclude fields that are default values here AND are not set in the existing glossary.
        #
        # In other words, a field will be included if:
        # 1. is set in the existing glossary or
        # 2. the value in the latest glossary is not a default value (and this isn't the top-level config)
        exclude = (exclude or set()).copy()

        # Exclude any fields that match the defaults.
        if defaults.owners is not None and defaults.owners == latest_elem.owners:
            exclude.add("owners")
        if isinstance(latest_elem, GlossaryTermConfig):
            if (
                defaults.source is not None
                and defaults.source == latest_elem.source_ref
            ):
                exclude.add("source_ref")
            if defaults.url == latest_elem.source_url:
                exclude.add("source_url")
            if defaults.source_type == latest_elem.term_source:
                exclude.add("term_source")

        if existing_elem is not None:
            # SUBTLE: We can drop the ID here because we know that existing_elem._urn
            # matches latest_elem._urn. If existing_elem is not None, then
            # we know the id field is redundant if it is not set in the existing glossary.
            exclude.add("id")

            # Make sure to include any fields that are explicitly set in the existing glossary.
            existing_set_keys = existing_elem.__fields_set__
            exclude -= existing_set_keys

        fields = latest_elem.dict(
            exclude=exclude,
            exclude_defaults=True,
            exclude_none=True,
        )

        return fields

    def _to_dict(
        latest_node: GlossaryNodeInterface,
        existing_node: GlossaryNodeInterface,
        defaults: DefaultConfig,
    ) -> dict:
        # Process terms.
        terms = []
        for latest_term_elem, existing_term_elem in _align_glossary_elements(
            latest_node.terms or [], existing_node.terms or []
        ):
            if latest_term_elem is None:
                terms.append(_DELETED_NODE_TOMBSTONE)
            else:
                terms.append(
                    _simple_elem_to_dict(latest_term_elem, existing_term_elem, defaults)
                )

        # Process nodes.
        nodes = []
        for latest_node_elem, existing_node_elem in _align_glossary_elements(
            latest_node.nodes or [], existing_node.nodes or []
        ):
            if latest_node_elem is None:
                nodes.append(_DELETED_NODE_TOMBSTONE)
            elif existing_node_elem is None:
                nodes.append(
                    _to_dict(
                        latest_node_elem,
                        existing_node=GlossaryNodeConfig(
                            id=latest_node_elem._urn,
                            name=latest_node_elem.name,
                            description="",
                        ),
                        defaults=defaults,
                    )
                )
            else:
                nodes.append(
                    _to_dict(
                        latest_node_elem,
                        existing_node_elem,
                        # Update defaults with the current node owner, if any.
                        # This way, the "owners" field cascades down to all child nodes.
                        defaults=defaults.copy(
                            update=(
                                dict(owners=existing_node_elem.owners)
                                if existing_node_elem.owners
                                else dict()
                            )
                        ),
                    )
                )

        # Retain other fields as-is.
        other_fields = _simple_elem_to_dict(
            latest_node, existing_node, defaults, exclude={"terms", "nodes"}
        )

        return {
            **other_fields,
            **({"terms": terms} if terms else {}),
            **({"nodes": nodes} if nodes else {}),
        }

    return _to_dict(latest_glossary, existing_glossary, defaults=existing_glossary)


def update_yml_to_match(
    infile: pathlib.Path,
    outfile: pathlib.Path,
    target: dict,
) -> None:
    yaml = YAML()
    yaml.preserve_quotes = True  # type: ignore[assignment]

    doc = yaml.load(infile)

    def _update_doc(doc, target, update_full):
        if isinstance(target, dict):
            if not isinstance(doc, dict):
                update_full(target)
            else:
                for key, value in target.items():
                    if key not in doc:
                        doc[key] = value
                    else:

                        def _update_value(v):
                            doc[key] = v

                        _update_doc(doc[key], value, _update_value)

        elif isinstance(target, list):
            if not isinstance(doc, list):
                update_full(doc, target)
            else:
                # We assume that the two lists are perfectly aligned, which the exception of deletions.
                elems_to_delete = []
                for i, value in enumerate(target):
                    if i >= len(doc):
                        doc.append(value)
                    elif doc[i] == _DELETED_NODE_TOMBSTONE:
                        elems_to_delete.append(i)
                    else:

                        def _update_value(v):
                            doc[i] = v

                        _update_doc(doc[i], value, _update_value)

                for i in reversed(elems_to_delete):
                    del doc[i]
        else:
            update_full(target)

    _update_doc(doc, target, None)

    # Guess existing indentation in the file so that we can preserve it.
    _, ind, bsi = ruamel.yaml.util.load_yaml_guess_indent(infile.read_text())
    yaml.width = 2**20  # type: ignore[assignment]
    yaml.indent(mapping=bsi, sequence=ind, offset=bsi)

    yaml.dump(doc, outfile)


def _update_glossary_file(
    file: str, enable_auto_id: bool, output: Optional[str], prune: bool = True
) -> None:
    if not output:
        output = file

    logger.info("Read the existing biz glossary file")
    existing_glossary = BusinessGlossaryFileSource.load_glossary_config(file)
    materialize_all_node_urns(existing_glossary, enable_auto_id=enable_auto_id)

    logger.info("Fetch the latest glossary from DataHub")
    top_level_nodes, top_level_terms = fetch_datahub_glossary()
    latest_glossary = existing_glossary.copy(
        update=dict(
            nodes=top_level_nodes,
            terms=top_level_terms,
        ),
        deep=True,
    )

    logger.info("Prune the latest glossary to only include file-managed nodes/terms")
    if prune:
        prune_latest_glossary(
            latest_glossary=latest_glossary,
            existing_glossary=existing_glossary,
        )

    logger.info("Recursively simplify urn references where possible.")
    path_to_id_map = populate_path_vs_id(latest_glossary)
    replace_urn_refs_with_paths(latest_glossary, path_to_id_map=path_to_id_map)

    logger.info(
        "Minimize diffs between the two glossaries using the field defaults and default config."
    )
    obj = glossary_to_dict_minimize_diffs(
        latest_glossary=latest_glossary,
        existing_glossary=existing_glossary,
    )

    logger.info("Serialize back into yaml.")
    update_yml_to_match(pathlib.Path(file), pathlib.Path(output), obj)


@click.group()
def cli():
    pass


@cli.command()
@click.option("--enable-auto-id", type=bool, required=True)
@click.option("--file", type=click.Path(exists=True, dir_okay=False), required=True)
@click.option("--prune", type=bool, default=False, required=False)
@click.option("--output", type=click.Path())
def update_glossary_file(
    file: str, enable_auto_id: bool, prune: bool, output: Optional[str]
) -> None:
    if not output:
        output = file

    _update_glossary_file(
        file, enable_auto_id=enable_auto_id, output=output, prune=prune
    )


@cli.command()
@click.option("--output", type=click.Path(), required=True)
@click.option("--default-source-ref")
@click.option("--default-source-url")
@click.option("--default-source-type")
def bootstrap_glossary_yml(
    output: str,
    default_source_ref: Optional[str],
    default_source_url: Optional[str],
    default_source_type: Optional[str],
) -> None:
    default_yml = """
# This file was auto-generated by the biz glossary CLI.
version: 1
owners: {}
"""
    if default_source_ref:
        default_yml += f"source: {default_source_ref}\n"
    if default_source_url:
        default_yml += f"url: {default_source_url}\n"
    if default_source_type:
        default_yml += f"source_type: {default_source_type}\n"

    pathlib.Path(output).write_text(default_yml)

    _update_glossary_file(output, enable_auto_id=True, output=output, prune=False)


if __name__ == "__main__":
    cli()
