import json
import logging
from pathlib import Path

import pytest
from langchain_anthropic import ChatAnthropic
from langchain_community.agent_toolkits import SlackToolkit
from langchain_core.messages import AIMessage, BaseMessage
from langchain_mcp_adapters.client import MultiServerMCPClient
from langchain_mcp_adapters.tools import load_mcp_tools
from langgraph.prebuilt import create_react_agent
from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client

import mcp_kinetica

LOG = logging.getLogger(__name__)
CLAUDE_MODEL = "claude-opus-4-1-20250805"
SERVER_KI_PATH = Path(mcp_kinetica.__file__).parent / "server_ki.py"

def log_messages(messages: list[BaseMessage]) -> None:
    for message in messages:
        log_message(message)

def log_message(message: BaseMessage) -> None:
    if isinstance(message, AIMessage):
        if isinstance(message.content, list):
            for item in message.content:
                if item['type'] == 'text':
                    LOG.info(f"[AIMessage] {item['text']}")
        else:
            LOG.info(f"[AIMessage] {message.content}")
    else:
        LOG.info(f"[{message.__class__.__name__}] {message.content}")


@pytest.mark.asyncio
async def test_client_sql() -> None:
    LOG.info(f"MCP path: {str(SERVER_KI_PATH)}")

    server_params = StdioServerParameters(
        command="python",
        args=[str(SERVER_KI_PATH)],
    )

    async with stdio_client(server_params) as (read, write), ClientSession(read, write) as session:
        await session.initialize()
        
        # Get tools
        tools = await load_mcp_tools(session)
        tool_names = [tool.name for tool in tools]
        LOG.info(f"Loaded tools: {tool_names}")

        model = ChatAnthropic(
            model=CLAUDE_MODEL,
            temperature=0,
            max_tokens=1024,
            max_retries=2,
        )

        prompt = ( "You are a database query assistant. Use the list_sql_contexts tool "
              + "to find available context and use generate_sql to generate SQL queries.")

        # Create and run the agent
        agent = create_react_agent(
            model=model,
            tools=tools,
            prompt=prompt,)
        
        agent_response = await agent.ainvoke(
            {"messages": "How many Starbucks locations are there?"})

        log_messages(agent_response.get("messages", []))


@pytest.mark.asyncio
async def test_multi_server_client() -> None:
    client = MultiServerMCPClient(
        connections={
            "server_ki": {
                "command": "python",
                # Make sure to update to the full absolute path to your math_server.py file
                "args": [str(SERVER_KI_PATH)],
                "transport": "stdio",
            }
        }
    )

    async with client.session("server_ki") as session:
        tools = await load_mcp_tools(session)
        tool_names = [tool.name for tool in tools]
        LOG.info(f"Loaded tools: {tool_names}")

        model = ChatAnthropic(
            model=CLAUDE_MODEL,
            temperature=0,
            max_tokens=1024,
            max_retries=2,
        )

        prompt = ( "You are a database query assistant. Use the list_sql_contexts tool "
              + "to find available context and use generate_sql to generate SQL queries.")

        # Create and run the agent
        agent = create_react_agent(
            model=model,
            tools=tools,
            prompt=prompt,)
        
        agent_response = await agent.ainvoke(
            {"messages": "How many Starbucks locations are there?"})

        log_messages(agent_response.get("messages", []))


def test_get_channelid_name_dict() -> None:
    toolkit = SlackToolkit()
    tools = toolkit.get_tools()

    tools_dict = {tool.name: tool for tool in tools}
    LOG.info(f"Loaded tools: {tools_dict.keys()}")

    get_channelid_name_dict = tools_dict["get_channelid_name_dict"]
    result_json = get_channelid_name_dict.invoke(input={})
    result = json.loads(result_json)
    for channel in result:
        LOG.info(f"Channel: {channel['name']} (ID: {channel['id']})")

    send_message = tools_dict["send_message"]
    result = send_message.invoke(input={"channel": "test-mcp", "message": "Hello World!"})

    LOG.info(f"Send message result: {result}")
    # execute the tool


def test_slack_agent() -> None:
    toolkit = SlackToolkit()
    tools = toolkit.get_tools()

    model = ChatAnthropic(
            model=CLAUDE_MODEL,
            temperature=0,
            max_tokens=1024,
            max_retries=2,
        )
    
    agent = create_react_agent(
        model=model, tools=tools)

    example_query = "When was the #general channel created? Please post the result to the test-mcp channel."

    events = agent.stream(
        {"messages": [("user", example_query)]},
        stream_mode="values",
    )

    for event in events:
        message = event["messages"][-1]
        #if message.type != "tool":  # mask sensitive information
        log_message(message)


@pytest.mark.asyncio
async def test_multi_server_client_with_slack() -> None:
    client = MultiServerMCPClient(
        connections={
            "server_ki": {
                "command": "python",
                # Make sure to update to the full absolute path to your math_server.py file
                "args": [str(SERVER_KI_PATH)],
                "transport": "stdio",
            }
        }
    )

    async with client.session("server_ki") as session:
        ki_tools = await load_mcp_tools(session)

        toolkit = SlackToolkit()
        slack_tools = toolkit.get_tools()
        tools = ki_tools + slack_tools

        tool_names = [tool.name for tool in tools]
        LOG.info(f"Loaded tools: {tool_names}")

        model = ChatAnthropic(
            model=CLAUDE_MODEL,
            temperature=0,
            max_tokens=1024,
            max_retries=2,
        )

        prompt = ( "You are a database query assistant. Use the list_sql_contexts tool "
              + "to find available context and use generate_sql to generate SQL queries. "
              + "Send the results to the test-mcp slack channel.")

        # Create and run the agent
        agent = create_react_agent(
            model=model,
            tools=tools,
            prompt=prompt,)
        
        agent_response = await agent.ainvoke(
            {"messages": "How many Starbucks locations are there?"})

        log_messages(agent_response.get("messages", []))
