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

def extract_condition_from_case(cei_condition):
    """Extract the condition part from CASE WHEN statement.
    
    Converts "CASE WHEN <condition> THEN true ELSE false END" to "<condition>"
    """
    if not cei_condition:
        return ""
    
    cei_condition = cei_condition.strip()
    
    # Check if it's a CASE WHEN statement
    if cei_condition.upper().startswith("CASE WHEN"):
        # Find "THEN true" or "THEN TRUE" to extract the condition
        # Handle both "THEN true" and "THEN TRUE"
        then_index = cei_condition.upper().find(" THEN ")
        if then_index > 0:
            # Extract the part after "CASE WHEN" and before "THEN"
            condition = cei_condition[9:then_index].strip()  # 9 is length of "CASE WHEN"
            return condition
    
    # If it doesn't match the pattern, return as is
    return cei_condition

# Main transformation logic
def transform_json(data, finding_title_map=None, framework_normalization_map=None, assessment_id_map=None):
    transformed = {}

    # Get cei_code from old CEI
    cei_code = data.get("cei_code", "")
    
    # Use assessment_id from CSV as the new id, fallback to cei_code if not found
    if assessment_id_map and cei_code in assessment_id_map:
        transformed["id"] = assessment_id_map[cei_code]
    else:
        transformed["id"] = cei_code
    transformed["title"] = data.get("cei_title", "")
    transformed["contributing_module"] = ["Reporting"]
    transformed["is_active"] = data.get("is_active", True)
    transformed["description"] = data.get("description", data.get("cei_description", ""))
    transformed["scope_entity"] = data.get("entity", [])
    transformed["scope_validation_steps"] = data.get("cei_description", [])
    transformed["scope_query"] = data.get("sql_query", "")
    
    # Replace table name in scope_query if scope_entity has only single entry
    scope_entity = transformed["scope_entity"]
    if len(scope_entity) == 1 and transformed["scope_query"]:
        entity_value = scope_entity[0].lower()
        old_table = "<%EI_PUBLISH_SCHEMA_NAME%>.sds_ei__publish_transformer__entity_inventory"
        new_table = f"<%EI_SCHEMA_NAME%>.sds_ei__{entity_value}__enrich"
        transformed["scope_query"] = transformed["scope_query"].replace(old_table, new_table)
    
    # Extract condition from CASE WHEN statement
    cei_condition = data.get("cei_condition", "")
    transformed["success_condition"] = extract_condition_from_case(cei_condition)
    
    # Set finding_primary_key based on scope_entity count
    scope_entity = transformed["scope_entity"]
    if len(scope_entity) > 1:
        transformed["finding_primary_key"] = "relationship_id"
    else:
        transformed["finding_primary_key"] = "p_id"
    
    # Map finding_title from CSV if available, otherwise use original or empty string
    # Use cei_code (not the new id) to look up finding_title since the map uses cei_code as key
    if finding_title_map and cei_code in finding_title_map:
        transformed["finding_title"] = finding_title_map[cei_code] if finding_title_map[cei_code] else ""
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

    transformed["exposure_category"] = "Control Gap"
    
    # Transform control_mapping and uppercase all values
    # Use normalized framework names if available
    # Exclude nist_csf_v1 and scf_2023_2 from old CEIs
    # Keep only scf_2023_4 from old CEIs
    framework_mapping = data.get("framework_mapping", {})
    control_mapping = {}
    for framework, values in framework_mapping.items():
        # Skip nist_csf_v1 from old CEIs
        if framework == "nist_csf_v1":
            continue
        
        # Skip scf_2023_2 from old CEIs, but keep scf_2023_4
        if framework == "scf_2023_2":
            continue
        
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

def load_cei_titles_data():
    """Load all CEI titles data from cei_titles.csv"""
    cei_data = {}
    csv_file = "cei_titles.csv"
    if os.path.exists(csv_file):
        with open(csv_file, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                # Support both old "id" and new "cei_id" column names for backward compatibility
                cei_id = row.get("cei_id", row.get("id", ""))
                if cei_id:
                    cei_data[cei_id] = {
                        "finding_title": row.get("finding_title", "").strip(),
                        "assessment_id": row.get("assessment_id", "").strip(),
                        "title": row.get("title", "").strip()
                    }
    return cei_data

def load_finding_title_map():
    """Load finding_title mappings from cei_titles.csv"""
    finding_title_map = {}
    cei_data = load_cei_titles_data()
    for cei_id, data in cei_data.items():
        finding_title_map[cei_id] = data["finding_title"]
    return finding_title_map

def load_assessment_id_map():
    """Load assessment_id mappings from cei_titles.csv"""
    assessment_id_map = {}
    cei_data = load_cei_titles_data()
    for cei_id, data in cei_data.items():
        assessment_id_map[cei_id] = data["assessment_id"]
    return assessment_id_map

def validate_cei_titles(specific_cei_ids=None):
    """Validate that all CEIs being migrated have finding_title and assessment_id values"""
    csv_file = "cei_titles.csv"
    
    if not os.path.exists(csv_file):
        print(f"\nError: '{csv_file}' not found. Please run generate_csvs.py first to create it.")
        return False
    
    # Load all CEI data
    cei_data = load_cei_titles_data()
    
    # Determine which CEIs need to be validated
    if specific_cei_ids:
        # Normalize specific CEI IDs for matching
        ceis_to_check = set()
        for cei_id in specific_cei_ids:
            cei_id = cei_id.strip()
            # Remove .json if present
            if cei_id.endswith('.json'):
                cei_id = cei_id[:-5]
            # Ensure CEI- prefix
            if not cei_id.startswith('CEI-'):
                cei_id = f"CEI-{cei_id}"
            ceis_to_check.add(cei_id)
    else:
        # Check all CEIs in the CSV
        ceis_to_check = set(cei_data.keys())
    
    missing_finding_titles = []
    missing_assessment_ids = []
    
    for cei_id in ceis_to_check:
        if cei_id not in cei_data:
            print(f"\nError: CEI '{cei_id}' not found in '{csv_file}'. Please add it first.")
            return False
        
        data = cei_data[cei_id]
        
        # Check if finding_title is missing
        if not data["finding_title"]:
            missing_finding_titles.append(cei_id)
        
        # Check if assessment_id is missing
        if not data["assessment_id"]:
            missing_assessment_ids.append(cei_id)
    
    # Report errors
    errors = []
    if missing_finding_titles:
        errors.append(f"  Missing finding_title for: {', '.join(missing_finding_titles)}")
    if missing_assessment_ids:
        errors.append(f"  Missing assessment_id for: {', '.join(missing_assessment_ids)}")
    
    if errors:
        print(f"\nError: The following CEIs in '{csv_file}' are missing required values:")
        for error in errors:
            print(error)
        print(f"\nPlease update '{csv_file}' with finding_title and assessment_id values for all CEIs before running the migration.")
        return False
    
    return True

def validate_framework_normalization():
    """Validate that all frameworks in detected_frameworks.csv have normalized_framework values"""
    frameworks_file = "detected_frameworks.csv"
    missing_normalizations = []
    
    # Frameworks that are excluded from migration and don't need normalized_framework values
    excluded_frameworks = {"nist_csf_v1", "scf_2023_2"}
    
    if not os.path.exists(frameworks_file):
        print(f"\nError: '{frameworks_file}' not found. Please run generate_csvs.py first to create it.")
        return False
    
    with open(frameworks_file, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            framework = row.get("framework", "").strip()
            normalized_framework = row.get("normalized_framework", "").strip()
            
            # Skip empty rows
            if not framework:
                continue
            
            # Skip excluded frameworks that don't need normalized_framework values
            if framework in excluded_frameworks:
                continue
            
            # Check if normalized_framework is missing or empty
            if not normalized_framework:
                missing_normalizations.append(framework)
    
    if missing_normalizations:
        print(f"\nError: The following frameworks in '{frameworks_file}' are missing normalized_framework values:")
        for framework in missing_normalizations:
            print(f"  - {framework}")
        print(f"\nPlease update '{frameworks_file}' with normalized_framework values for all frameworks before running the migration.")
        return False
    
    return True

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
    
    # Validate framework normalization before proceeding
    if not validate_framework_normalization():
        return
    
    # Validate CEI titles (finding_title and assessment_id) before proceeding
    if not validate_cei_titles(specific_cei_ids):
        return
    
    # Load mappings from CSV files
    finding_title_map = load_finding_title_map()
    framework_normalization_map = load_framework_normalization_map()
    assessment_id_map = load_assessment_id_map()
    
    if finding_title_map:
        print(f"Loaded {len(finding_title_map)} finding_title mappings from cei_titles.csv")
    if framework_normalization_map:
        print(f"Loaded {len(framework_normalization_map)} framework normalizations from detected_frameworks.csv")
    if assessment_id_map:
        print(f"Loaded {len(assessment_id_map)} assessment_id mappings from cei_titles.csv")
    
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
                    transformed_data = transform_json(original_data, finding_title_map, framework_normalization_map, assessment_id_map)

                    # Use the new id (assessment_id) as the filename instead of the original filename
                    new_filename = f"{transformed_data.get('id', 'unknown')}.json"
                    output_path = os.path.join(output_folder, new_filename)
                    with open(output_path, 'w', encoding='utf-8') as out_f:
                        json.dump(transformed_data, out_f, indent=2)

                    print(f"Processed: {filename} -> {new_filename}")
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
        print("1. Migrate all CEIs")
        print("2. Migrate specific CEIs")
        print("3. Exit")
        print("="*50)
        
        choice = input("\nEnter your choice (1-3): ").strip()
        
        if choice == "1":
            migrate_ceis()
        elif choice == "2":
            cei_input = input("\nEnter CEI IDs (comma-separated, e.g., CEI-1424,CEI-1426 or 1424,1426): ").strip()
            if cei_input:
                cei_ids = [cei.strip() for cei in cei_input.split(",")]
                migrate_ceis(specific_cei_ids=cei_ids)
            else:
                print("\nNo CEI IDs provided. Cancelling migration.")
        elif choice == "3":
            print("\nExiting...")
            break
        else:
            print("\nInvalid choice. Please enter 1, 2, or 3.")

if __name__ == "__main__":
    show_menu()
