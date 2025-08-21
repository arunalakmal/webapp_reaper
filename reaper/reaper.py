import argparse
from collections.abc import Set
import logging
import json
import os
from pathlib import Path
import re
import sys
from typing import List, Dict, Any, Optional
from azure.identity import DefaultAzureCredential
from azure.identity import ClientSecretCredential
from azure.mgmt.containerregistry import ContainerRegistryManagementClient  
from azure.mgmt.web import WebSiteManagementClient
from azure.containerregistry import ContainerRegistryClient

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ------------------- SILENCE LOGGING -------------------
logging.getLogger("azure.containerregistry").setLevel(logging.WARNING)
logging.getLogger("azure.core.pipeline.policies.http_logging_policy").setLevel(logging.WARNING)
logging.getLogger("azure.identity").setLevel(logging.WARNING)
# -------------------------------------------------------

class Reaper:
    def __init__(self, subscription_id: str, resource_group_name: str, registry_name: str):
        self.subscription_id = subscription_id
        self.resource_group_name = resource_group_name
        self.registry_name = registry_name

        # self.credential = DefaultAzureCredential()
        self.credential = ClientSecretCredential(
            tenant_id=os.environ["AZURE_TENANT_ID"],
            client_id=os.environ["AZURE_CLIENT_ID"],
            client_secret=os.environ["AZURE_CLIENT_SECRET"]
        )
        self.container_registry_client = ContainerRegistryManagementClient(self.credential, self.subscription_id)
        self.web_site_client = WebSiteManagementClient(self.credential, self.subscription_id)
        
        # Initialize the data client for registry operations
        registry_url = f"https://{self.registry_name}"
        self.container_registry_data_client = ContainerRegistryClient(registry_url, self.credential)

    @staticmethod
    def load_configs(config_path: str) -> List[str]:
        config_file = Path(config_path)

        if not config_file.exists():
            raise FileNotFoundError(f"Configuration file {config_path} does not exist.")
        
        try: 
            with open(config_file, 'r', encoding='utf-8') as file:
                config = json.load(file)

            web_apps = []

            if isinstance(config, dict):
                if 'webApps' in config:
                    if isinstance(config['webApps'], list):
                        web_apps = [str(app) for app in config['webApps']]
                        logger.info(f"Web apps to protect: {web_apps}")
                    else:
                        raise ValueError("webApps should be a list.")
            else:
                raise ValueError("Configuration file should contain a JSON object.")
        except json.JSONDecodeError as e:
            logger.error(f"Error decoding JSON from the configuration file {config_path}.")
            raise json.JSONDecodeError(f"Configuration file {config_path} is not a valid JSON file. file: {e.msg}" ,e.doc, e.pos)
        except Exception as e:
            logger.error(f"Unexpected error occurred while processing the configuration file {config_path}: {e}")
            raise ValueError(f"Configuration file {config_path} is not a valid JSON file.")
        
        return web_apps
        
    def extract_web_apps_images(self, registry_url: str) -> Optional[Dict[str, str]]:
        if not registry_url:
            logger.error("Registry URL is not provided.")
            return None
        try:
            pattern = r'^(?:.*\.azurecr\.io/)?([^:]+):(.+)$'
            match = re.match(pattern, registry_url)
            if match:
                repository_name = match.group(1)
                tag = match.group(2)
                full_image_name = f"{repository_name}:{tag}"
                logger.info(f"Extracted image name: {full_image_name}")
                return {
                    "repository": repository_name,
                    "tag": tag,
                    "full_image_name": full_image_name
                }
            else:
                logger.error(f"Registry URL {registry_url} does not match the expected pattern.")
                return None
        except Exception as e:
            logger.error(f"Failed to extract web apps images: {e}")
            return None
    
    def get_webapps_slots_images(self, web_app_name: str) -> tuple[Set[str], Set[str]]:
        """
        Get images used by web app and return both full image names and repository names.
        Returns: (set of full image names, set of repository names)
        """
        images = set()
        repositories = set()
        
        try:
            logger.info(f"Retrieving web app slots for {web_app_name}...")
            logger.info(f"Web app production slot: {web_app_name}")
            prod_slot = self.web_site_client.web_apps.get(self.resource_group_name, web_app_name)
            
            # Check Linux container configuration
            if hasattr(prod_slot.site_config, 'linux_fx_version') and prod_slot.site_config.linux_fx_version:
                linux_fx_version = prod_slot.site_config.linux_fx_version
                if linux_fx_version.startswith('DOCKER|'):
                    image_url = linux_fx_version.replace('DOCKER|', '')
                    image_extracted = self.extract_web_apps_images(image_url)
                    if image_extracted:
                        images.add(image_extracted['full_image_name'])
                        repositories.add(image_extracted['repository'])
                        logger.info(f"Production slot image extracted from a linux container: {image_extracted['full_image_name']}")
            else:
                logger.warning(f"No Linux FX version found for production slot of web app {web_app_name}.")
                
            # Check Windows container configuration    
            if hasattr(prod_slot.site_config, 'windows_fx_version') and prod_slot.site_config.windows_fx_version:
                windows_fx_version = prod_slot.site_config.windows_fx_version
                if windows_fx_version.startswith('DOCKER|'):
                    image_url = windows_fx_version.replace('DOCKER|', '')
                    image_extracted = self.extract_web_apps_images(image_url)
                    if image_extracted:
                        images.add(image_extracted['full_image_name'])
                        repositories.add(image_extracted['repository'])
                        logger.info(f"Production slot image extracted from a windows container: {image_extracted['full_image_name']}")
            else:
                logger.warning(f"No Windows FX version found for production slot of web app {web_app_name}.")
        except Exception as e:
            logger.error(f"Failed to retrieve production slot images for web app {web_app_name}: {e}")
        
        try:    
            logger.info(f"Retrieving deployment slots for {web_app_name}...")
            slots = self.web_site_client.web_apps.list_slots(self.resource_group_name, web_app_name)
            for slot in slots:
                slot_name = slot.name.split('/')[-1]
                logger.info(f"Checking slot Name {slot_name}")

                try: 
                    slot_config = self.web_site_client.web_apps.get_configuration_slot(self.resource_group_name, web_app_name, slot_name)
                    
                    # Check Linux container configuration for slot
                    if hasattr(slot_config, 'linux_fx_version') and slot_config.linux_fx_version:
                        linux_fx_version = slot_config.linux_fx_version
                        if linux_fx_version.startswith('DOCKER|'):
                            image_url = linux_fx_version.replace('DOCKER|', '')
                            image_extracted = self.extract_web_apps_images(image_url)
                            if image_extracted:
                                images.add(image_extracted['full_image_name'])
                                repositories.add(image_extracted['repository'])
                                logger.info(f"Slot {slot_name} image extracted from a linux container: {image_extracted['full_image_name']}")
                                
                    # Check Windows container configuration for slot
                    if hasattr(slot_config, 'windows_fx_version') and slot_config.windows_fx_version:
                        windows_fx_version = slot_config.windows_fx_version
                        if windows_fx_version.startswith('DOCKER|'):
                            image_url = windows_fx_version.replace('DOCKER|', '')
                            image_extracted = self.extract_web_apps_images(image_url)
                            if image_extracted:
                                images.add(image_extracted['full_image_name'])
                                repositories.add(image_extracted['repository'])
                                logger.info(f"Slot {slot_name} image extracted from a windows container: {image_extracted['full_image_name']}")
                except Exception as e:
                    logger.error(f"Failed to retrieve configuration for slot {slot_name} of web app {web_app_name}: {e}")
        except Exception as e:
            logger.error(f"Failed to retrieve slot images for web app {web_app_name}: {e}")
            
        return images, repositories

    def get_all_web_apps_data(self, web_apps: List[str]) -> tuple[Set[str], Set[str]]:
        """
        Get all images and repositories used by web apps.
        Returns: (set of all protected images, set of all repositories to clean)
        """
        all_images = set()
        all_repositories = set()
        
        for web_app in web_apps:
            logger.info(f"Processing web app: {web_app}")
            images, repositories = self.get_webapps_slots_images(web_app)
            logger.info(f"Images extracted for web app {web_app}: {images}")
            logger.info(f"Repositories found for web app {web_app}: {repositories}")
            
            if images:
                all_images.update(images)
            if repositories:
                all_repositories.update(repositories)
                
        logger.info(f"Total protected images across all web apps: {len(all_images)}")
        logger.info(f"Total repositories to clean: {len(all_repositories)}")
        logger.info(f"Protected images: {sorted(all_images)}")
        logger.info(f"Repositories to clean: {sorted(all_repositories)}")
        
        return all_images, all_repositories
    
    def get_acr_repository_tags(self, repository_name: str) -> List[str]:
        try:
            logger.info(f"Retrieving tags for repository {repository_name} in ACR {self.registry_name}...")
            tags = self.container_registry_data_client.list_tag_properties(repository_name)
            tag_list = [tag.name for tag in tags]
            logger.info(f"Tags retrieved for repository {repository_name}: {tag_list}")
            return tag_list
        except Exception as e:
            logger.error(f"Failed to retrieve tags for repository {repository_name}: {e}")
            return []

    def identify_unused_images(self, repositories: Set[str], protected_images: Set[str]) -> Dict[str, List[str]]:
        """
        Identify unused images in discovered repositories that are not protected by web apps.
        Returns a dictionary mapping repository names to lists of unused tags.
        """
        unused_images = {}
        
        for repository in repositories:
            logger.info(f"Analyzing repository: {repository}")
            all_tags = self.get_acr_repository_tags(repository)
            
            unused_tags = []
            protected_tags = []
            
            for tag in all_tags:
                full_image_name = f"{repository}:{tag}"
                if full_image_name not in protected_images:
                    unused_tags.append(tag)
                    logger.info(f"Found unused image: {full_image_name}")
                else:
                    protected_tags.append(tag)
                    logger.info(f"Protecting image (in use by web app): {full_image_name}")
            
            if unused_tags:
                unused_images[repository] = unused_tags
                logger.info(f"Repository {repository} has {len(unused_tags)} unused tags out of {len(all_tags)} total tags")
                logger.info(f"  - Protected tags ({len(protected_tags)}): {sorted(protected_tags)}")
                logger.info(f"  - Unused tags ({len(unused_tags)}): {sorted(unused_tags)}")
            else:
                logger.info(f"Repository {repository} has no unused tags (all {len(all_tags)} tags are in use)")
        
        return unused_images

    def delete_unused_images(self, unused_images: Dict[str, List[str]], dry_run: bool = True) -> Dict[str, Dict[str, Any]]:
        """
        Delete unused images from the container registry.
        
        Args:
            unused_images: Dictionary mapping repository names to lists of unused tags
            dry_run: If True, only simulate the deletion without actually deleting
        
        Returns:
            Dictionary with deletion results for each repository
        """
        deletion_results = {}
        
        for repository, tags in unused_images.items():
            deletion_results[repository] = {
                'attempted': len(tags),
                'successful': 0,
                'failed': 0,
                'errors': []
            }
            
            logger.info(f"{'[DRY RUN] ' if dry_run else ''}Processing repository: {repository}")
            
            for tag in tags:
                try:
                    full_image_name = f"{repository}:{tag}"
                    logger.info(f"{'[DRY RUN] ' if dry_run else ''}Deleting image: {full_image_name}")
                    
                    if not dry_run:
                        # Delete the tag from the repository
                        self.container_registry_data_client.delete_tag(repository, tag)
                        logger.info(f"Successfully deleted image: {full_image_name}")
                    else:
                        logger.info(f"[DRY RUN] Would delete image: {full_image_name}")
                    
                    deletion_results[repository]['successful'] += 1
                    
                except Exception as e:
                    error_msg = f"Failed to delete {repository}:{tag}: {str(e)}"
                    logger.error(error_msg)
                    deletion_results[repository]['failed'] += 1
                    deletion_results[repository]['errors'].append(error_msg)
        
        return deletion_results

    def cleanup_unused_manifests(self, repositories: Set[str], dry_run: bool = True):
        """
        Clean up untagged manifests (manifests without any tags pointing to them).
        This is useful after deleting tags as manifests may become orphaned.
        """
        logger.info(f"{'[DRY RUN] ' if dry_run else ''}Starting cleanup of untagged manifests...")
        
        for repository in repositories:
            try:
                logger.info(f"{'[DRY RUN] ' if dry_run else ''}Checking for untagged manifests in {repository}")
                
                # List all manifests in the repository
                manifests = self.container_registry_data_client.list_manifest_properties(repository)
                
                for manifest in manifests:
                    # Check if manifest has any tags
                    if not manifest.tags:
                        logger.info(f"{'[DRY RUN] ' if dry_run else ''}Found untagged manifest: {manifest.digest}")
                        
                        if not dry_run:
                            try:
                                self.container_registry_data_client.delete_manifest(repository, manifest.digest)
                                logger.info(f"Successfully deleted untagged manifest: {manifest.digest}")
                            except Exception as e:
                                logger.error(f"Failed to delete manifest {manifest.digest}: {e}")
                        else:
                            logger.info(f"[DRY RUN] Would delete untagged manifest: {manifest.digest}")
                            
            except Exception as e:
                logger.error(f"Failed to cleanup untagged manifests in {repository}: {e}")

    def print_summary(self, deletion_results: Dict[str, Dict[str, Any]], protected_images: Set[str], repositories_cleaned: Set[str]):
        """Print a comprehensive summary of the cleanup operation."""
        total_attempted = sum(result['attempted'] for result in deletion_results.values())
        total_successful = sum(result['successful'] for result in deletion_results.values())
        total_failed = sum(result['failed'] for result in deletion_results.values())
        
        logger.info("=" * 70)
        logger.info("AZURE CONTAINER REGISTRY CLEANUP SUMMARY")
        logger.info("=" * 70)
        logger.info(f"Registry: {self.registry_name}.azurecr.io")
        logger.info(f"Resource Group: {self.resource_group_name}")
        logger.info(f"Subscription: {self.subscription_id}")
        logger.info("-" * 70)
        logger.info(f"Repositories analyzed: {len(repositories_cleaned)}")
        logger.info(f"Protected images: {len(protected_images)}")
        logger.info(f"Total images processed for deletion: {total_attempted}")
        logger.info(f"Successfully deleted: {total_successful}")
        logger.info(f"Failed to delete: {total_failed}")
        logger.info("-" * 70)
        
        # Show protected images
        if protected_images:
            logger.info("Protected Images (currently in use by web apps):")
            for image in sorted(protected_images):
                logger.info(f"  ✓ {image}")
            logger.info("-" * 70)
        
        # Show detailed results per repository
        for repository, result in deletion_results.items():
            logger.info(f"Repository: {repository}")
            logger.info(f"  Total tags processed: {result['attempted']}")
            logger.info(f"  Successfully deleted: {result['successful']}")
            logger.info(f"  Failed to delete: {result['failed']}")
            
            if result['errors']:
                logger.info("  Errors encountered:")
                for error in result['errors']:
                    logger.info(f"    ✗ {error}")
            logger.info("-" * 70)
        
        # Show repositories that were analyzed but had no unused images
        all_repos_with_results = set(deletion_results.keys())
        repos_with_no_unused = repositories_cleaned - all_repos_with_results
        if repos_with_no_unused:
            logger.info("Repositories with no unused images:")
            for repo in sorted(repos_with_no_unused):
                logger.info(f"  ✓ {repo} (all images are in use)")
            logger.info("-" * 70)
        
        logger.info(f"Cleanup completed. Space potentially freed by removing {total_successful} unused images.")
        logger.info("=" * 70)
            

def main():
    parser = argparse.ArgumentParser(
        description="Azure Container Registry Reaper - Auto-discover and clean up unused container images",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument(
        '--subscription-id',
        required=True,
        help='Azure Subscription ID'
    )
    parser.add_argument(
        '--resource-group',
        required=True,
        help='Azure Resource Group Name'
    )
    parser.add_argument(
        '--registry-name',
        required=True,
        help='Azure Container Registry Name'
    )
    parser.add_argument(
        '--config-path',
        default='reaper/webapp.json',
        help='Path to the configuration file containing web apps to protect'
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        default=True,
        help='Perform a dry run without actually deleting images (default: True)'
    )
    parser.add_argument(
        '--execute',
        action='store_true',
        help='Actually execute the deletion (overrides dry-run)'
    )
    parser.add_argument(
        '--cleanup-manifests',
        action='store_true',
        help='Also cleanup untagged manifests after deleting images'
    )

    args = parser.parse_args()  

    # Determine if this should be a dry run
    dry_run = not args.execute

    if not all([args.subscription_id, args.resource_group, args.registry_name, args.config_path]):
        parser.error("All arguments --subscription-id, --resource-group, --registry-name, and --config-path are required.")    

    try:
        reaper = Reaper(args.subscription_id, args.resource_group, args.registry_name)
        web_apps = reaper.load_configs(args.config_path)
        
        if not web_apps:
            logger.warning("No web apps specified in configuration file. Nothing to analyze.")
            sys.exit(0)
        
        logger.info("=" * 50)
        logger.info("Azure Container Registry Reaper Started")
        logger.info("=" * 50)
        logger.info(f"Registry: {args.registry_name}.azurecr.io")
        logger.info(f"Resource Group: {args.resource_group}")
        logger.info(f"Mode: {'DRY RUN (use --execute to actually delete)' if dry_run else 'EXECUTE (will actually delete images)'}")
        logger.info(f"Web apps to analyze: {web_apps}")
        logger.info("=" * 50)
        
        # Get all images currently used by web apps (protected) and discover repositories
        protected_images, repositories_to_clean = reaper.get_all_web_apps_data(web_apps)
        
        if not repositories_to_clean:
            logger.warning("No repositories discovered from web apps. Nothing to clean.")
            sys.exit(0)
        
        # Identify unused images in the discovered repositories
        unused_images = reaper.identify_unused_images(repositories_to_clean, protected_images)
        
        # Initialize deletion_results for summary
        deletion_results = {}
        
        if not unused_images:
            logger.info("No unused images found in any repository. Nothing to delete.")
        else:
            total_unused = sum(len(tags) for tags in unused_images.values())
            logger.info(f"Found {total_unused} unused images across {len(unused_images)} repositories")
            
            # Delete unused images
            deletion_results = reaper.delete_unused_images(unused_images, dry_run)
            
            # Cleanup untagged manifests if requested
            if args.cleanup_manifests:
                reaper.cleanup_unused_manifests(repositories_to_clean, dry_run)
        
        # Print comprehensive summary
        reaper.print_summary(
            deletion_results, 
            protected_images, 
            repositories_to_clean
        )
        
        logger.info("Container registry cleanup completed successfully.")
        
    except Exception as e:
        logger.error(f"Failed to execute reaper: {e}")
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")
        sys.exit(1)

if __name__ == "__main__":
    main()