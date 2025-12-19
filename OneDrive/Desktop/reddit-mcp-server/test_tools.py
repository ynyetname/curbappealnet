"""
Test script for the Reddit MCP Server
"""

import asyncio
from reddit_mcp_server.main import search_reddit, get_subreddit_info

async def test_tools():
    """Test the Reddit MCP tools."""
    print("Testing Reddit MCP Server tools...\n")
    
    # Test search_reddit
    print("1. Testing search_reddit tool:")
    try:
        result = await search_reddit({
            "query": "python programming",
            "limit": 3
        })
        print("✓ search_reddit test passed")
        print(f"Result preview: {result[0].text[:200]}...\n")
    except Exception as e:
        print(f"✗ search_reddit test failed: {e}\n")
    
    # Test get_subreddit_info
    print("2. Testing get_subreddit_info tool:")
    try:
        result = await get_subreddit_info({
            "subreddit": "python"
        })
        print("✓ get_subreddit_info test passed")
        print(f"Result preview: {result[0].text[:200]}...\n")
    except Exception as e:
        print(f"✗ get_subreddit_info test failed: {e}\n")
    
    print("Testing complete!")

if __name__ == "__main__":
    asyncio.run(test_tools())