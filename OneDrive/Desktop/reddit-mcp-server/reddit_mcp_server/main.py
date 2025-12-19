from mcp.server.fastmcp import FastMCP
import httpx

mcp = FastMCP("Reddit mcp server")

reddit_search_api = "https://www.reddit.com/search.json"
reddit_getpost_api = "https://www.reddit.com/r/{subreddit}/comments/{post_id}.json"
reddit_gethotposts_api = "https://www.reddit.com/r/{subreddit}/hot.json"
reddit_newpost_api = "https://www.reddit.com/r/{subreddit}/new.json"
reddit_topposts_api = "https://www.reddit.com/r/{subreddit}/top.json"
reddit_userprofile_api = "https://www.reddit.com/user/{username}.json"
reddit_subreddit_info_api = "https://www.reddit.com/r/{subreddit}/about.json"

User_agent = "reddit-mcp-server/1.0"

@mcp.tool("search_reddit")
async def search_reddit(query: str) -> str:
    """Search reddit posts"""
    try:
        async with httpx.AsyncClient() as client:
            response = await client.get(reddit_search_api, params={"q": query}, headers={"User-Agent": User_agent})
            response.raise_for_status()
            data = response.json()
            posts = data.get("data", {}).get("children", [])
            if not posts:
                return "No posts found."
            results = []
            for post in posts:
                post_data = post.get("data", {})
                title = post_data.get("title", "No title")
                url = post_data.get("url", "No URL")
                results.append(f"Title: {title}\nURL: {url}\n")
            return "\n".join(results)
    except Exception as e:
        return f"Error: {str(e)}"

@mcp.tool("get_reddit_post")
async def post_reddit(subreddit: str, post_id: str) -> str:
    """Get specific reddit post"""
    try:
        async with httpx.AsyncClient() as client:
            response = await client.get(reddit_getpost_api.format(subreddit=subreddit, post_id=post_id), headers={"User-Agent": User_agent})
            response.raise_for_status()
            data = response.json()
            post_data = data[0]["data"]["children"][0]["data"]
            title = post_data.get("title", "No title")
            url = post_data.get("url", "No URL")
            return f"Title: {title}\nURL: {url}\n"
    except Exception as e:
        return f"Error: {str(e)}"

@mcp.tool("get_hot_posts")
async def get_hot_posts(subreddit: str) -> str:
    """Get hot posts from subreddit"""
    try:
        async with httpx.AsyncClient() as client:
            response = await client.get(reddit_gethotposts_api.format(subreddit=subreddit), headers={"User-Agent": User_agent})
            response.raise_for_status()
            data = response.json()
            posts = data.get("data", {}).get("children", [])
            if not posts:
                return "No posts found."
            results = []
            for post in posts:
                post_data = post.get("data", {})
                title = post_data.get("title", "No title")
                url = post_data.get("url", "No URL")
                results.append(f"Title: {title}\nURL: {url}\n")
            return "\n".join(results)
    except Exception as e:
        return f"Error: {str(e)}"

@mcp.tool("get_new_posts")
async def get_new_posts(subreddit: str) -> str:
    """Get new posts from subreddit"""
    try:
        async with httpx.AsyncClient() as client:
            response = await client.get(reddit_newpost_api.format(subreddit=subreddit), headers={"User-Agent": User_agent})
            response.raise_for_status()
            data = response.json()
            posts = data.get("data", {}).get("children", [])
            if not posts:
                return "No posts found."
            results = []
            for post in posts:
                post_data = post.get("data", {})
                title = post_data.get("title", "No title")
                url = post_data.get("url", "No URL")
                results.append(f"Title: {title}\nURL: {url}\n")
            return "\n".join(results)
    except Exception as e:
        return f"Error: {str(e)}"

@mcp.tool("get_top_posts")
async def get_top_posts(subreddit: str) -> str:
    """Get top posts from subreddit"""
    try:
        async with httpx.AsyncClient() as client:
            response = await client.get(reddit_topposts_api.format(subreddit=subreddit), headers={"User-Agent": User_agent})
            response.raise_for_status()
            data = response.json()
            posts = data.get("data", {}).get("children", [])
            if not posts:
                return "No posts found."
            results = []
            for post in posts:
                post_data = post.get("data", {})
                title = post_data.get("title", "No title")
                url = post_data.get("url", "No URL")
                results.append(f"Title: {title}\nURL: {url}\n")
            return "\n".join(results)
    except Exception as e:
        return f"Error: {str(e)}"

@mcp.tool("get_user_profile")
async def get_user_profile(username: str) -> str:
    """Get reddit user profile"""
    try:
        async with httpx.AsyncClient() as client:
            response = await client.get(reddit_userprofile_api.format(username=username), headers={"User-Agent": User_agent})
            response.raise_for_status()
            data = response.json()
            user_data = data.get("data", {})
            if not user_data:
                return "User not found."
            name = user_data.get("name", "No name")
            karma = user_data.get("total_karma", "No karma")
            created_utc = user_data.get("created_utc", "No creation date")
            return f"Username: {name}\nTotal Karma: {karma}\nAccount Created (UTC): {created_utc}\n"
    except Exception as e:
        return f"Error: {str(e)}"

@mcp.tool("get_subreddit_info")
async def get_subreddit_info(subreddit: str) -> str:
    """Get subreddit information"""
    try:
        async with httpx.AsyncClient() as client:
            response = await client.get(reddit_subreddit_info_api.format(subreddit=subreddit), headers={"User-Agent": User_agent})
            response.raise_for_status()
            data = response.json()
            subreddit_data = data.get("data", {})
            if not subreddit_data:
                return "Subreddit not found."
            name = subreddit_data.get("display_name", "No name")
            title = subreddit_data.get("title", "No title")
            description = subreddit_data.get("public_description", "No description")
            subscribers = subreddit_data.get("subscribers", "No subscribers")
            return f"Subreddit: {name}\nTitle: {title}\nDescription: {description}\nSubscribers: {subscribers}\n"
    except Exception as e:
        return f"Error: {str(e)}"

if __name__ == "__main__":
    mcp.run(transport="http")