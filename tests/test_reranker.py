import os

import pytest

from hirag_prod._llm import EmbeddingService
from hirag_prod.schema import Entity
from hirag_prod.storage.lancedb import LanceDB
from hirag_prod.storage.retrieval_strategy_provider import RetrievalStrategyProvider


@pytest.mark.asyncio
async def test_lancedb():
    strategy_provider = RetrievalStrategyProvider()
    lance_db = await LanceDB.create(
        embedding_func=EmbeddingService().create_embeddings,
        db_url="kb/test.db",
        strategy_provider=strategy_provider,
    )

    # Load text from test_reranker.txt, a shorten version of test.text
    with open(os.path.join(os.path.dirname(__file__), "test_reranker.txt"), "r") as f:
        test_to_embed = f.read()

    await lance_db.upsert_text(
        text_to_embed=test_to_embed,
        properties={
            "text": test_to_embed,
            "document_key": "test",
            "filename": "test_reranker.txt",
            "private": True,
        },
        table_name="test",
        mode="overwrite",
    )

    async_table = await lance_db.upsert_text(
        text_to_embed="Repeat, Hello, world!",
        properties={
            "text": "Repeat, Hello, world!",
            "document_key": "test_append",
            "filename": "in_memory_test",
            "private": True,
        },
        table_name="test",
        mode="append",
    )
    
    recall = await lance_db.query(
        query="tell me about bitcoin",
        table=async_table,
        topk=3,
        document_list=["test"],
        require_access="private",
        columns_to_select=["text", "document_key", "filename", "private"],
        distance_threshold=100,  # a very high threshold to ensure all results are returned
    )
    assert len(recall) == 2
    assert recall[0]["text"] == test_to_embed
    assert recall[1]["text"] == "Repeat, Hello, world!"


