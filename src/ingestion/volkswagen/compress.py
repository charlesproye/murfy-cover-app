import asyncio
from src.core.compressor import Compressor


def main():
    asyncio.run(compress())


async def compress():
    compressor = Compressor(brand_prefix="volkswagen")
    print(compressor._s3._settings)
    await compressor.run()


if __name__ == "__main__":
    main()

