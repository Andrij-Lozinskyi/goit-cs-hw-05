import asyncio
import logging
import argparse
from pathlib import Path
import aiofiles
import aiofiles.os
import os
import shutil
from typing import Set, Dict
from collections import defaultdict

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('file_sorter.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Список системних директорій, які будем пропускати
SYSTEM_DIRS = {
    'System Volume Information',
    '$Recycle.Bin',
    'Config.Msi',
    'Documents and Settings',
    'Program Files',
    'Program Files (x86)',
    'Windows',
    'Recovery'
}

async def create_directory(path: Path) -> None:
    try:
        await aiofiles.os.makedirs(path, exist_ok=True)
    except Exception as e:
        logger.error(f"Error creating directory {path}: {e}")
        raise

async def get_all_files(source_dir: Path) -> Set[Path]:
    files = set()
    try:
        if source_dir.name in SYSTEM_DIRS:
            logger.info(f"Skipping system directory: {source_dir}")
            return files

        for entry in os.scandir(str(source_dir)):
            try:
                entry_path = Path(entry.path)
                
                if entry.is_dir() and entry.name in SYSTEM_DIRS:
                    logger.info(f"Skipping system directory: {entry_path}")
                    continue
                    
                if entry.is_file():
                    try:
                        with open(entry_path, 'rb'):
                            files.add(entry_path)
                    except PermissionError:
                        logger.warning(f"No access to file: {entry_path}")
                        continue
                elif entry.is_dir() and not entry.name.startswith('.'):
                    try:
                        subdir_files = await get_all_files(entry_path)
                        files.update(subdir_files)
                    except PermissionError:
                        logger.warning(f"No access to directory: {entry_path}")
                        continue
            except PermissionError:
                logger.warning(f"No access to entry: {entry.path}")
                continue
    except PermissionError:
        logger.warning(f"No access to directory: {source_dir}")
    except Exception as e:
        logger.error(f"Error scanning directory {source_dir}: {e}")
    return files

async def move_file(source: Path, dest_dir: Path) -> None:
    try:
        extension = source.suffix[1:].lower() if source.suffix else 'no_extension'
        target_dir = dest_dir / extension
        await create_directory(target_dir)
        
        destination = target_dir / source.name
        
        counter = 1
        while os.path.exists(destination):
            stem = source.stem
            new_name = f"{stem}_{counter}{source.suffix}"
            destination = target_dir / new_name
            counter += 1
        
        try:
            shutil.move(str(source), str(destination))
            logger.info(f"Successfully moved: {source} -> {destination}")
        except PermissionError:
            logger.warning(f"No permission to move file: {source}")
        except Exception as e:
            logger.error(f"Error moving file {source}: {e}")
    except Exception as e:
        logger.error(f"Error processing file {source}: {e}")

async def process_files(source_dir: Path, dest_dir: Path) -> None:
    try:
        files = await get_all_files(source_dir)
        
        if not files:
            logger.warning("No accessible files found to process")
            return
            
        extension_counts: Dict[str, int] = defaultdict(int)
        for file in files:
            ext = file.suffix[1:].lower() if file.suffix else 'no_extension'
            extension_counts[ext] += 1
        
        total_files = len(files)
        processed_files = 0
        
        semaphore = asyncio.Semaphore(10)
        
        async def move_with_progress(file: Path) -> None:
            nonlocal processed_files
            async with semaphore:
                await move_file(file, dest_dir)
                processed_files += 1
                if processed_files % 10 == 0:
                    logger.info(f"Progress: {processed_files}/{total_files} files processed")
        
        tasks = [move_with_progress(file) for file in files]
        await asyncio.gather(*tasks)
        
        logger.info(f"Processing completed. Total files processed: {total_files}")
        logger.info("Files processed by extension:")
        for ext, count in sorted(extension_counts.items()):
            logger.info(f"  .{ext}: {count} files")
            
    except Exception as e:
        logger.error(f"Error during file processing: {e}")

def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description='Asynchronous file sorter by extension'
    )
    parser.add_argument(
        'source_dir',
        type=str,
        help='Path to source directory'
    )
    parser.add_argument(
        'dest_dir',
        type=str,
        help='Path to destination directory'
    )
    return parser.parse_args()

async def main():
    args = parse_arguments()
    
    source_dir = Path(args.source_dir).resolve()
    dest_dir = Path(args.dest_dir).resolve()
    
    if not source_dir.exists():
        logger.error(f"Source directory does not exist: {source_dir}")
        return
        
    if source_dir == dest_dir:
        logger.error("Source and destination directories cannot be the same")
        return
    
    await create_directory(dest_dir)
    
    try:
        await process_files(source_dir, dest_dir)
    except KeyboardInterrupt:
        logger.info("Process interrupted by user")
    except Exception as e:
        logger.error(f"Process failed: {e}")
    finally:
        logger.info("Process finished")

if __name__ == "__main__":
    asyncio.run(main())