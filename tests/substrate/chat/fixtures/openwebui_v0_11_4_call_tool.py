# Excerpt from the official OpenWebUI v0.11.4 MCP client; call_tool is
# byte-identical at v0.11.0.
# Source: https://github.com/open-webui/open-webui/blob/v0.11.4/backend/open_webui/utils/mcp/client.py
# Upstream LICENSE and LICENSE_HISTORY are retained under ops/openwebui/.
"""Smallest executable fixture preserving the pinned native MCP call path."""

from typing import Optional


class MCPClient:
    def __init__(self):
        self.session = None

    async def call_tool(self, function_name: str, function_args: dict) -> Optional[dict]:
        if not self.session:
            raise RuntimeError('MCP client is not connected.')

        result = await self.session.call_tool(function_name, function_args)
        if not result:
            raise Exception('No result returned from MCP tool call.')

        result_dict = result.model_dump(mode='json')
        result_content = result_dict.get('content', {})

        if result.isError:
            raise Exception(result_content)
        else:
            return result_content
