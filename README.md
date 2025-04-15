# HiRAG-MCP

Python server implementing Model Context Protocol (MCP) for [HiRAG](https://github.com/hhy-huang/HiRAG).

## Features

- Naive RAG retrieval
- HiRAG RAG retrieval


## API

### Tools
- **naive_search**
    - Perform a naive search over the knowledge base.
    - Inputs:
        - `query`: The search query text
        - `max_tokens` (optional): Optional maximum tokens for the response

- **hi_search**
    - Perform a hybrid search combining both local and global knowledge over the knowledge base. (default to use)
    - Inputs:
        - `query`: The search query text
        - `max_tokens` (optional): Optional maximum tokens for the response

## Test

### uv (recommended)

Make sure uv is installed, if not:
```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

Then create the virtual environment and install dependencies using uv as below:
```bash
uv venv
source .venv/bin/activate
uv sync
```

### Running the server

Note that we already have a `test` folder and `test.txt` file, which are the results after indexing `test.txt` with `hi_indexing_openai.py`. You can directly use the following command to run the MCP server and test the tools:

```bash
mcp dev server.py
```

You can also use `hi_indexing_openai.py` to index your own text files. Remember to adjust the `working_dir` in config.yaml accordingly.

## License

This MCP server is licensed under the MIT License. This means you are free to use, modify, and distribute the software, subject to the terms and conditions of the MIT License. For more details, please see the LICENSE file in the project repository.
