import os
import json
import csv

# Path settings
input_folder = 'Old CEIs'     # folder with original JSON files
output_folder = 'New CEIs'

# Ensure output directory exists
os.makedirs(output_folder, exist_ok=True)

# Set to collect all distinct framework keys
detected_frameworks = set()

# List to collect titles for CSV export
titles_list = []

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
def transform_json(data):
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
    framework_mapping = data.get("framework_mapping", {})
    control_mapping = {}
    for framework, values in framework_mapping.items():
        if isinstance(values, list):
            control_mapping[framework] = [v.upper() if isinstance(v, str) else v for v in values]
        else:
            control_mapping[framework] = values
    transformed["control_mapping"] = control_mapping

    return transformed

# Process all JSON files
for filename in os.listdir(input_folder):
    if filename.endswith(".json"):
        with open(os.path.join(input_folder, filename), 'r', encoding='utf-8') as f:
            try:
                original_data = json.load(f)

                # Collect framework keys from framework_mapping
                framework_mapping = original_data.get("framework_mapping", {})
                if isinstance(framework_mapping, dict):
                    detected_frameworks.update(framework_mapping.keys())

                # Transform structure (before removing ui_config, as it's needed for finding_config)
                transformed_data = transform_json(original_data)

                # Collect title for CSV export
                titles_list.append({
                    "id": transformed_data.get("id", ""),
                    "title": transformed_data.get("title", "")
                })

                # Remove unwanted fields from original_data (not needed anymore, but keeping for consistency)
                for field in remove_fields:
                    original_data.pop(field, None)

                # Save transformed JSON
                output_path = os.path.join(output_folder, filename)
                with open(output_path, 'w', encoding='utf-8') as out_f:
                    json.dump(transformed_data, out_f, indent=2)

                print(f"Processed: {filename}")

            except json.JSONDecodeError:
                print(f"Skipping invalid JSON: {filename}")

# Save detected frameworks to text file
if detected_frameworks:
    frameworks_file = "detected frameworks.txt"
    with open(frameworks_file, 'w', encoding='utf-8') as f:
        # Sort frameworks for consistent output
        sorted_frameworks = sorted(detected_frameworks)
        for framework in sorted_frameworks:
            f.write(f"{framework}\n")
    print(f"\nDetected {len(detected_frameworks)} distinct framework(s). Saved to '{frameworks_file}'")
else:
    print("\nNo frameworks detected in any CEI files.")

# Save titles to CSV file
if titles_list:
    csv_file = "cei_titles.csv"
    with open(csv_file, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=["id", "title"])
        writer.writeheader()
        # Sort by ID for consistent output
        sorted_titles = sorted(titles_list, key=lambda x: x["id"])
        writer.writerows(sorted_titles)
    print(f"\nSaved {len(titles_list)} title(s) to '{csv_file}'")
else:
    print("\nNo titles collected to save.")
 
