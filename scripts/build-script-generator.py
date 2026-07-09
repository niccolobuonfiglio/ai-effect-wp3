#!/usr/bin/env python3
"""
Build Script Generator for AI-Effect Use Cases

This script generates a build_and_tag.sh script for a use case that:
1. Builds all service images using docker compose
2. Tags images with consistent naming for use in platform exports
"""

import yaml
import argparse
from pathlib import Path


class BuildScriptGenerator:
    def __init__(self, use_case_dir):
        self.use_case_dir = Path(use_case_dir)
        self.use_case_name = self.use_case_dir.name

    def load_docker_compose(self):
        """Load docker-compose.yml to extract service names"""
        compose_file = self.use_case_dir / 'docker-compose.yml'

        if not compose_file.exists():
            raise FileNotFoundError(f"docker-compose.yml not found in {self.use_case_dir}")

        with open(compose_file, 'r') as f:
            compose_config = yaml.safe_load(f)

        return compose_config

    def extract_services(self, compose_config):
        """Extract service names from docker-compose config"""
        services = []

        if 'services' not in compose_config:
            raise ValueError("No services found in docker-compose.yml")

        for service_name in compose_config['services'].keys():
            services.append(service_name)

        return services

    def generate_build_script(self, services):
        """Generate build_and_tag.sh script content"""

        # Generate tag commands for each service
        tag_commands = []
        image_list = []

        for service in services:
            # Docker compose creates images as: {directory_name}-{service_name}
            base_image = f"{self.use_case_name}-{service}"
            tagged_image = f"{base_image}:latest"

            tag_commands.append(f"docker tag {base_image} {tagged_image}")
            image_list.append(tagged_image)

        script_content = f"""#!/bin/bash
set -e

echo "Building Docker images for {self.use_case_name}..."

# Build all services using docker compose
docker compose build

echo ""
echo "Tagging images with :latest..."

# Tag images with :latest for export compatibility
""" + "\n".join(tag_commands) + """

echo ""
echo "Successfully built and tagged all images:"
""" + "\n".join(f'echo "  - {img}"' for img in image_list) + """
echo ""
echo "Images are ready for:"
echo "  1. Local development: docker compose up"
echo "  2. Platform export generation: python scripts/onboarding-export-generator.py"
"""

        return script_content

    def write_build_script(self, script_content):
        """Write build_and_tag.sh to use case directory"""
        script_path = self.use_case_dir / 'build_and_tag.sh'

        with open(script_path, 'w') as f:
            f.write(script_content)

        # Make script executable
        script_path.chmod(0o755)

        print(f"Generated: {script_path}")
        return script_path

    def generate(self):
        """Main generation process"""
        print(f"Generating build script for: {self.use_case_name}")
        print(f"Location: {self.use_case_dir}")

        # Load and parse docker-compose.yml
        compose_config = self.load_docker_compose()
        services = self.extract_services(compose_config)

        print(f"Found {len(services)} services: {', '.join(services)}")

        # Generate and write script
        script_content = self.generate_build_script(services)
        script_path = self.write_build_script(script_content)

        print("\nUsage:")
        print(f"  cd {self.use_case_dir}")
        print(f"  ./build_and_tag.sh")

        return script_path


def main():
    parser = argparse.ArgumentParser(
        description='Generate build_and_tag.sh script for a use case'
    )
    parser.add_argument(
        'use_case_dir',
        help='Path to use case directory containing docker-compose.yml'
    )

    args = parser.parse_args()

    try:
        generator = BuildScriptGenerator(args.use_case_dir)
        generator.generate()

        print("\n" + "="*60)
        print("SUCCESS")
        print("="*60)
        print("\nNext steps:")
        print(f"1. Build images: cd {args.use_case_dir} && ./build_and_tag.sh")
        print("2. Test locally: docker compose up")
        print("3. Generate export: python scripts/onboarding-export-generator.py ...")

    except Exception as e:
        print(f"Error: {e}")
        return 1

    return 0


if __name__ == '__main__':
    exit(main())
