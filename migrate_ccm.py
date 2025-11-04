import os
import json

# Path settings
input_folder = 'input_folder'     # folder with original JSON files
output_folder = 'output_folder'

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
def transform_json(data):
    transformed = {}

    # Field mappings
    transformed["id"] = data.get("cei_code", "")
    transformed["title"] = data.get("cei_title", "")
    transformed["contributing_module"] = data.get("contributing_module", [])
    transformed["is_active"] = data.get("is_active", True)
    transformed["description"] = data.get("description", data.get("cei_description", ""))
    transformed["scope_entity"] = data.get("entity", [])
    transformed["scope_validation_steps"] = data.get("cei_description", [])
    transformed["scope_query"] = data.get("sql_query", "")
    transformed["success_condition"] = data.get("cei_condition", "")
    transformed["finding_primary_key"] = data.get("finding_primary_key", "")
    transformed["finding_title"] = data.get("finding_title", "")

    # Add a default finding_config template if not present
    transformed["finding_config"] = [
        {
            "title": "",
            "expression": "",
            "finding_evidence": True
        }
    ]

    transformed["exposure_category"] = data.get("exposure_category", "")
    transformed["control_mapping"] = data.get("framework_mapping", {})

    return transformed

# Process all JSON files
for filename in os.listdir(input_folder):
    if filename.endswith(".json"):
        with open(os.path.join(input_folder, filename), 'r', encoding='utf-8') as f:
            try:
                original_data = json.load(f)

                # Remove unwanted fields
                for field in remove_fields:
                    original_data.pop(field, None)

                # Transform structure
                transformed_data = transform_json(original_data)

                # Save transformed JSON
                output_path = os.path.join(output_folder, filename)
                with open(output_path, 'w', encoding='utf-8') as out_f:
                    json.dump(transformed_data, out_f, indent=2)

                print(f"Processed: {filename}")

            except json.JSONDecodeError:
                print(f"Skipping invalid JSON: {filename}")
 