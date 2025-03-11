from telethon import TelegramClient
from telethon.tl.functions.channels import JoinChannelRequest
import asyncio
import os
from dotenv import load_dotenv

async def main():
    load_dotenv()
    
    client = TelegramClient(
        os.getenv('SESSION_NAME'),
        int(os.getenv('API_ID')),
        os.getenv('API_HASH')
    )
    
    await client.start()
    
    # Load channels from existing file
    with open('channels.txt') as f:
        channels = [line.strip() for line in f if line.strip() and not line.startswith('#')]
    
    print(f"Attempting to join {len(channels)} channels...")
    
    joined = []
    failed = []
    
    for channel in channels:
        try:
            entity = await client.get_entity(channel)
            await client(JoinChannelRequest(entity))
            joined.append(channel)
            print(f"Joined {channel}")
            await asyncio.sleep(5)  # Rate limit protection
        except Exception as e:
            failed.append((channel, str(e)))
            print(f"Failed {channel}: {str(e)}")
    
    print("\nResults:")
    print(f"Successfully joined {len(joined)} channels")
    print(f"Failed to join {len(failed)} channels")
    
    if failed:
        print("\nFailed channels:")
        for channel, error in failed:
            print(f"- {channel}: {error}")
    
    await client.disconnect()

if __name__ == '__main__':
    asyncio.run(main()) 