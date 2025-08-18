import argparse
from config import Settings
from collector_mssql import Collector


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--country", required=True, help="e.g., IT, RS")
    parser.add_argument("--scene", required=True, help="e.g., trap")
    args = parser.parse_args()

    settings = Settings.load("config.yaml")
    scenes = settings.country_scenes.get(args.country, [])
    scene_cfg = next((s for s in scenes if s.name == args.scene), None)
    if not scene_cfg:
        raise SystemExit(f"No scene '{args.scene}' configured for {args.country}")

    collector = Collector(settings)
    collector.crawl_scene(args.country, scene_cfg)
    print(f"✅ Collected playlists + tracks for {args.country}/{args.scene} into MSSQL.")


if __name__ == "__main__":
    main()
