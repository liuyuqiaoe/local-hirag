import logging
from dataclasses import dataclass, field
from typing import Optional


from hirag_prod._llm import gpt_4o_mini_complete, openai_embedding
from hirag_prod.chunk import BaseChunk, FixTokenChunk
from hirag_prod.entity import BaseEntity, VanillaEntity
from hirag_prod.loader import load_document
from hirag_prod.storage import (
    BaseGDB,
    BaseVDB,
    LanceDB,
    NetworkXGDB,
    RetrievalStrategyProvider,
)

logger = logging.getLogger("HiRAG")


@dataclass
class HiRAG:
    # Chunk documents
    chunker: BaseChunk = field(
        default_factory=lambda: FixTokenChunk(chunk_size=1200, chunk_overlap=200)
    )

    # Entity extraction
    entity_extractor: BaseEntity = field(
        default_factory=lambda: VanillaEntity.create(extract_func=gpt_4o_mini_complete)
    )

    # Storage
    vdb: BaseVDB = field(default=None)
    gdb: BaseGDB = field(
        default_factory=lambda: NetworkXGDB.create(
            path="kb/hirag.gpickle",
            llm_func=gpt_4o_mini_complete,
        )
    )

    @classmethod
    async def create(cls, **kwargs):
        if kwargs.get("vdb") is None:
            lancedb = await LanceDB.create(
                embedding_func=openai_embedding,
                db_url="kb/hirag.db",
                strategy_provider=RetrievalStrategyProvider(),
            )
            kwargs["vdb"] = lancedb
        return cls(**kwargs)

    async def insert_to_kb(
        self,
        document_path: str,
        content_type: str,
        document_meta: Optional[dict] = None,
        loader_configs: Optional[dict] = None,
    ):
        # Load the document from the document path
        logger.info(f"Loading the document from the document path: {document_path}")
        documents = load_document(
            document_path,
            content_type,
            document_meta,
            loader_configs,
            loader_type="mineru",
        )
        logger.info(f"Loaded {len(documents)} documents")

        logger.info("Chunking the documents")
        # TODO: Handle the concurrent upsertion
        for document in documents:
            chunks = self.chunker.chunk(document)
            # TODO: Handle the concurrent upsertion
            for chunk in chunks:
                await self.vdb.upsert_text(
                    text_to_embed=chunk.page_content,
                    properties={
                        "document_key": chunk.id,
                        "text": chunk.page_content,
                        **chunk.metadata.__dict__,
                    },
                    table_name="chunks",
                    mode="overwrite",
                )
            entities = await self.entity_extractor.entity(chunks)
            # Store to lancedb for now
            for entity in entities:
                await self.vdb.upsert_text(
                    text_to_embed=entity.metadata.description,
                    properties={
                        "document_key": entity.id,
                        "text": entity.page_content,
                        **entity.metadata.__dict__,
                    },
                    table_name="entities",
                    mode="overwrite",
                )

            # extract relations
            relations = await self.entity_extractor.relation(chunks, entities)
            # TODO: handle the concurrent upsertion
            for relation in relations:
                await self.gdb.upsert_relation(relation)
            # dump the graph
            await self.gdb.dump()

    async def query_chunks(self, query: str, topk: int = 10):
        return await self.vdb.query(
            query=query,
            table=await self.vdb.db.open_table("chunks"),
            topk=topk,
            require_access="public",
            columns_to_select=["text", "document_key", "filename", "private"],
            distance_threshold=100,  # a very high threshold to ensure all results are returned
        )

    async def query_entities(self, query: str, topk: int = 10):
        return await self.vdb.query(
            query=query,
            table=await self.vdb.db.open_table("entities"),
            topk=topk,
            columns_to_select=["text", "document_key", "entity_type", "description"],
            distance_threshold=100,  # a very high threshold to ensure all results are returned
        )

    async def query_relations(self, query: str, topk: int = 10):
        # search the entities
        recall_entities = await self.query_entities(query, topk)
        recall_entities = [entity['document_key'] for entity in recall_entities]
        # search the relations
        recall_neighbors = []
        recall_edges = []
        for entity in recall_entities:
            neighbors, edges = await self.gdb.query_one_hop(entity)
            recall_neighbors.extend(neighbors)
            recall_edges.extend(edges)
        return recall_neighbors, recall_edges
    
    async def query_all(self, query: str, topk: int = 10):
        # search chunks
        recall_chunks = await self.query_chunks(query, topk)
        # search entities
        recall_entities = await self.query_entities(query, topk)
        # search relations
        recall_neighbors, recall_edges = await self.query_relations(query, topk)
        # merge the results
        return {
            "chunks": recall_chunks,
            "entities": recall_entities,
            "neighbors": recall_neighbors,
            "edges": recall_edges,
        }
