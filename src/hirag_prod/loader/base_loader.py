#! /usr/bin/env python3

from abc import ABC
from typing import List, Optional, Type

from langchain_core.document_loaders import BaseLoader as LangchainBaseLoader

from hirag_prod._utils import compute_mdhash_id
from hirag_prod.schema import File, FileMetadata

from .markify_loader import MarkifyClient


class BaseLoader(ABC):
    """Base class for all loaders"""

    loader_type: Type[LangchainBaseLoader]
    loader_markify: Type[MarkifyClient]
    # additional metadata to add to the loaded raw documents
    page_number_key: str = "page_number"

    def _load(self, document_path: str, **loader_args) -> List[File]:
        raw_docs = self.loader_type(document_path, **loader_args).load()
        docs = []
        for i, doc in enumerate(raw_docs, start=1):
            # Only set page number and doc hash here
            doc = File(
                id=compute_mdhash_id(doc.page_content, prefix="doc-"),
                page_content=doc.page_content,
                metadata=FileMetadata(
                    page_number=i,
                    type="pdf",  # Default to pdf since we're using markify
                    filename="",  # Empty string as placeholder
                    uri="",  # Empty string as placeholder
                ),
            )
            docs.append(doc)

        return docs
        return raw_docs

    def _load_markify(self, document_path: str, mode="advanced") -> List[File]:
        raw_text = self.loader_markify.convert_pdf(file_path=document_path, mode=mode)

        # Split text into chunks based on token limit
        text_chunks = self.loader_markify.split_text_by_tokens(
            raw_text, max_tokens=6000
        )

        # Create Document objects for each chunk
        docs = []
        for i, chunk in enumerate(text_chunks, start=1):
            # Only set page number and doc hash here
            doc = File(
                id=compute_mdhash_id(chunk.strip(), prefix="doc-"),
                page_content=chunk,
                metadata=FileMetadata(
                    page_number=i,
                    type="pdf",  # Default to pdf since we're using markify
                    filename="",  # Empty string as placeholder
                    uri="",  # Empty string as placeholder
                ),
            )
            docs.append(doc)

        return docs

    def load(
        self, document_path: str, document_meta: Optional[dict] = None, **loader_args
    ) -> list[File]:
        """Load document and set the metadata of the output

        Args:
            document_path (str): The document path for langchain loader to use.
            document_meta (Optional[dict]): The document metadata to set to the output.
            loader_args (dict): The arguments for the langchain loader.

        Returns:
            list[File]: Raw documents.
        """
        if document_meta is None:
            document_meta = {}
        raw_docs = self._load(document_path, **loader_args)
        self._set_doc_metadata(raw_docs, document_meta)
        return raw_docs

    def load_markify(
        self, document_path: str, document_meta: Optional[dict] = None, mode="advanced"
    ) -> List[File]:
        """Load document with markify(MinerU) and set the metadata of the output

        Args:
            document_path (str): The document path for markify loader to use.
            document_meta (Optional[dict]): The document metadata to set to the output.
            mode (str): The mode for the markify loader.

        Returns:
            list[File]: Raw documents.
        """
        if document_meta is None:
            document_meta = {}
        raw_docs = self._load_markify(document_path, mode)
        self._set_doc_metadata(raw_docs, document_meta)
        return raw_docs

    def _set_doc_metadata(self, docs: List[File], document_meta: dict) -> List[File]:
        for doc in docs:
            # Keep the original page number
            page_number = doc.metadata.page_number

            # Create metadata with all required fields
            metadata = FileMetadata(
                page_number=page_number,
                type=document_meta.get("type", "pdf"),  # Default to pdf
                filename=document_meta.get("filename", ""),
                uri=document_meta.get("uri", ""),
            )
            doc.metadata = metadata
        return docs
