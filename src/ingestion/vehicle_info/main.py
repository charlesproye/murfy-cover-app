import asyncio
from other import main as other_main
from tesla import main as tesla_main


if __name__ == "__main__":
    asyncio.run(tesla_main())
    asyncio.run(other_main())
