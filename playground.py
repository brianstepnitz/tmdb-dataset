import asyncio
import random

async def parent(index: int):
    num_children = random.randint(2, 7)
    print(f"> Parent {index} started with {num_children} children")
    await asyncio.gather(*(child(index, i) for i in range(num_children)))
    print(f"< Parent {index} finished {num_children} children")

async def child(parent_index: int, index: int):
    sleep_secs = random.uniform(0.1, 0.5)
    print(f">> Child {parent_index}.{index} started with sleep_secs={sleep_secs:.2f}")
    await asyncio.sleep(sleep_secs)
    print(f"<< Child {parent_index}.{index} finished sleep_secs={sleep_secs:.2f}")

async def source_generator(num_sources: int):
    for i in range(num_sources):
        print(f"> Starting source {i}")
        await parent(i)
        print(f"< Finished source {i}")
        yield f"Source {i} completed"

async def main():
    async for source in source_generator(20):
        print(source)

if __name__ == "__main__":
    asyncio.run(main())