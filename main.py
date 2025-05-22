import asyncio
import logging  # 添加日志模块

from hirag_prod import HiRAG

# 设置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def index():
    index = await HiRAG.create()
    document_path = f"tests/test.txt"
    content_type = "csv"
    document_meta = {
        "type": "csv",
        "filename": "test.txt",
        "uri": document_path,
        "private": False,
    }
    """
    await index.insert_to_kb(
        document_path=document_path,
        content_type=content_type,
        document_meta=document_meta,
    )
    """
    print(await index.query_all("tell me about bitcoin"))


if __name__ == "__main__":
    asyncio.run(index())
