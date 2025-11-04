import os
import json
import csv

# Path settings
input_folder = 'Old CEIs'     # folder with original JSON files
output_folder = 'New CEIs'

# Ensure output directory exists
os.makedirs(output_folder, exist_ok=True)

# Fields to remove entirely
remove_fields = {
    "cei_measure",
    "scope_source",
    "status_source",
    "cei_type",
    "internal_control_category",
    "ui_config"
}

# Main transformation logic
def transform_json(data, finding_title_map=None, framework_normalization_map=None):
    transformed = {}

    # Field mappings
    transformed["id"] = data.get("cei_code", "")
    transformed["title"] = data.get("cei_title", "")
    transformed["contributing_module"] = ["Reporting"]
    transformed["is_active"] = data.get("is_active", True)
    transformed["description"] = data.get("description", data.get("cei_description", ""))
    transformed["scope_entity"] = data.get("entity", [])
    transformed["scope_validation_steps"] = data.get("cei_description", [])
    transformed["scope_query"] = data.get("sql_query", "")
    transformed["success_condition"] = data.get("cei_condition", "")
    
    # Set finding_primary_key based on scope_entity count
    scope_entity = transformed["scope_entity"]
    if len(scope_entity) > 1:
        transformed["finding_primary_key"] = "relationship_id"
    else:
        transformed["finding_primary_key"] = "p_id"
    
    # Map finding_title from CSV if available, otherwise use original or empty string
    cei_id = transformed["id"]
    if finding_title_map and cei_id in finding_title_map:
        transformed["finding_title"] = finding_title_map[cei_id] if finding_title_map[cei_id] else ""
    else:
        transformed["finding_title"] = data.get("finding_title", "")

    # Transform ui_config.mapping into finding_config
    finding_config = []
    ui_config = data.get("ui_config", {})
    mappings = ui_config.get("mapping", []) or ui_config.get("mappings", [])
    
    if mappings:
        for mapping_item in mappings:
            # Skip cei_status field
            data_field = mapping_item.get("data_field", "")
            if data_field == "cei_status":
                continue
            finding_config.append({
                "title": mapping_item.get("data_label", ""),
                "expression": data_field,
                "finding_evidence": True
            })
    else:
        # Add a default finding_config template if no mappings present
        finding_config = [
            {
                "title": "",
                "expression": "",
                "finding_evidence": True
            }
        ]
    
    transformed["finding_config"] = finding_config

    transformed["exposure_category"] = data.get("exposure_category", "")
    
    # Transform control_mapping and uppercase all values
    # Use normalized framework names if available
    framework_mapping = data.get("framework_mapping", {})
    control_mapping = {}
    for framework, values in framework_mapping.items():
        # Use normalized framework name if available, otherwise use original
        normalized_framework = framework
        if framework_normalization_map and framework in framework_normalization_map:
            normalized_framework = framework_normalization_map[framework] if framework_normalization_map[framework] else framework
        
        if isinstance(values, list):
            control_mapping[normalized_framework] = [v.upper() if isinstance(v, str) else v for v in values]
        else:
            control_mapping[normalized_framework] = values
    transformed["control_mapping"] = control_mapping

    return transformed

def detect_frameworks():
    """Detect all distinct frameworks from Old CEIs and save to CSV"""
    detected_frameworks = set()
    
    print("\nDetecting frameworks from Old CEIs...")
    for filename in os.listdir(input_folder):
        if filename.endswith(".json"):
            with open(os.path.join(input_folder, filename), 'r', encoding='utf-8') as f:
                try:
                    original_data = json.load(f)
                    framework_mapping = original_data.get("framework_mapping", {})
                    if isinstance(framework_mapping, dict):
                        detected_frameworks.update(framework_mapping.keys())
                except json.JSONDecodeError:
                    print(f"Skipping invalid JSON: {filename}")
    
    # Save detected frameworks to CSV file
    if detected_frameworks:
        frameworks_file = "detected_frameworks.csv"
        # Check if file exists and read existing data
        existing_data = {}
        if os.path.exists(frameworks_file):
            with open(frameworks_file, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    existing_data[row["framework"]] = row.get("normalized_framework", "")
        
        with open(frameworks_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=["framework", "normalized_framework"])
            writer.writeheader()
            # Sort frameworks for consistent output
            sorted_frameworks = sorted(detected_frameworks)
            for framework in sorted_frameworks:
                # Preserve existing normalized_framework if it exists
                normalized = existing_data.get(framework, "")
                writer.writerow({"framework": framework, "normalized_framework": normalized})
        print(f"\nDetected {len(detected_frameworks)} distinct framework(s). Saved to '{frameworks_file}'")
    else:
        print("\nNo frameworks detected in any CEI files.")

def extract_titles():
    """Extract all titles from Old CEIs and save to CSV"""
    titles_list = []
    
    print("\nExtracting titles from Old CEIs...")
    for filename in os.listdir(input_folder):
        if filename.endswith(".json"):
            with open(os.path.join(input_folder, filename), 'r', encoding='utf-8') as f:
                try:
                    original_data = json.load(f)
                    transformed_data = transform_json(original_data)
                    
                    # Collect title for CSV export
                    titles_list.append({
                        "id": transformed_data.get("id", ""),
                        "title": transformed_data.get("title", ""),
                        "finding_title": ""
                    })
                except json.JSONDecodeError:
                    print(f"Skipping invalid JSON: {filename}")
    
    # Save titles to CSV file
    if titles_list:
        csv_file = "cei_titles.csv"
        # Check if file exists and read existing finding_title data
        existing_data = {}
        if os.path.exists(csv_file):
            with open(csv_file, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    existing_data[row["id"]] = row.get("finding_title", "")
        
        with open(csv_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=["id", "title", "finding_title"])
            writer.writeheader()
            # Sort by ID for consistent output
            sorted_titles = sorted(titles_list, key=lambda x: x["id"])
            for title_row in sorted_titles:
                # Preserve existing finding_title if it exists
                cei_id = title_row["id"]
                if cei_id in existing_data:
                    title_row["finding_title"] = existing_data[cei_id]
                writer.writerow(title_row)
        print(f"\nSaved {len(titles_list)} title(s) to '{csv_file}'")
    else:
        print("\nNo titles collected to save.")

def load_finding_title_map():
    """Load finding_title mappings from cei_titles.csv"""
    finding_title_map = {}
    csv_file = "cei_titles.csv"
    if os.path.exists(csv_file):
        with open(csv_file, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                cei_id = row.get("id", "")
                finding_title = row.get("finding_title", "")
                if cei_id:
                    finding_title_map[cei_id] = finding_title
    return finding_title_map

def load_framework_normalization_map():
    """Load framework normalization mappings from detected_frameworks.csv"""
    framework_normalization_map = {}
    frameworks_file = "detected_frameworks.csv"
    if os.path.exists(frameworks_file):
        with open(frameworks_file, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                framework = row.get("framework", "")
                normalized_framework = row.get("normalized_framework", "")
                if framework and normalized_framework:
                    framework_normalization_map[framework] = normalized_framework
    return framework_normalization_map

def migrate_ceis(specific_cei_ids=None):
    """Migrate CEIs from Old CEIs to New CEIs with CSV mappings
    
    Args:
        specific_cei_ids: Optional list of CEI IDs to migrate. If None, migrates all CEIs.
    """
    if specific_cei_ids:
        print(f"\nMigrating specific CEIs: {', '.join(specific_cei_ids)}")
    else:
        print("\nMigrating all CEIs from Old CEIs to New CEIs...")
    
    # Load mappings from CSV files
    finding_title_map = load_finding_title_map()
    framework_normalization_map = load_framework_normalization_map()
    
    if finding_title_map:
        print(f"Loaded {len(finding_title_map)} finding_title mappings from cei_titles.csv")
    if framework_normalization_map:
        print(f"Loaded {len(framework_normalization_map)} framework normalizations from detected_frameworks.csv")
    
    # Normalize specific CEI IDs for matching (remove .json, ensure CEI- prefix)
    target_filenames = set()
    if specific_cei_ids:
        for cei_id in specific_cei_ids:
            cei_id = cei_id.strip()
            # Remove .json if present
            if cei_id.endswith('.json'):
                cei_id = cei_id[:-5]
            # Ensure CEI- prefix
            if not cei_id.startswith('CEI-'):
                cei_id = f"CEI-{cei_id}"
            target_filenames.add(f"{cei_id}.json")
    
    processed_count = 0
    skipped_count = 0
    for filename in os.listdir(input_folder):
        if filename.endswith(".json"):
            # Skip if specific CEIs requested and this file is not in the list
            if specific_cei_ids and filename not in target_filenames:
                continue
            
            with open(os.path.join(input_folder, filename), 'r', encoding='utf-8') as f:
                try:
                    original_data = json.load(f)

                    # Transform structure with mappings
                    transformed_data = transform_json(original_data, finding_title_map, framework_normalization_map)

                    # Save transformed JSON
                    output_path = os.path.join(output_folder, filename)
                    with open(output_path, 'w', encoding='utf-8') as out_f:
                        json.dump(transformed_data, out_f, indent=2)

                    print(f"Processed: {filename}")
                    processed_count += 1

                except json.JSONDecodeError:
                    print(f"Skipping invalid JSON: {filename}")
                    skipped_count += 1
    
    if specific_cei_ids and processed_count < len(specific_cei_ids):
        not_found = len(specific_cei_ids) - processed_count
        print(f"\nWarning: {not_found} CEI(s) not found or could not be processed.")
    
    print(f"\nMigration complete! Processed {processed_count} CEI file(s).")
    if skipped_count > 0:
        print(f"Skipped {skipped_count} invalid file(s).")

def show_menu():
    """Display menu and handle user selection"""
    while True:
        print("\n" + "="*50)
        print("CEI Migration Tool")
        print("="*50)
        print("1. Detect all frameworks")
        print("2. Extract all titles")
        print("3. Migrate all CEIs")
        print("4. Migrate specific CEIs")
        print("5. Exit")
        print("="*50)
        
        choice = input("\nEnter your choice (1-5): ").strip()
        
        if choice == "1":
            detect_frameworks()
        elif choice == "2":
            extract_titles()
        elif choice == "3":
            migrate_ceis()
        elif choice == "4":
            cei_input = input("\nEnter CEI IDs (comma-separated, e.g., CEI-1424,CEI-1426 or 1424,1426): ").strip()
            if cei_input:
                cei_ids = [cei.strip() for cei in cei_input.split(",")]
                migrate_ceis(specific_cei_ids=cei_ids)
            else:
                print("\nNo CEI IDs provided. Cancelling migration.")
        elif choice == "5":
            print("\nExiting...")
            break
        else:
            print("\nInvalid choice. Please enter 1, 2, 3, 4, or 5.")

if __name__ == "__main__":
    show_menu()
