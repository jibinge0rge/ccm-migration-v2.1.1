import os
import json
import csv
from migrate_ccm import transform_json

# Path settings
input_folder = 'Old CEIs'     # folder with original JSON files

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
                        "cei_id": transformed_data.get("id", ""),
                        "assessment_id": "",
                        "title": transformed_data.get("title", ""),
                        "finding_title": ""
                    })
                except json.JSONDecodeError:
                    print(f"Skipping invalid JSON: {filename}")
    
    # Save titles to CSV file
    if titles_list:
        csv_file = "cei_titles.csv"
        # Check if file exists and read existing finding_title and assessment_id data
        existing_data = {}
        if os.path.exists(csv_file):
            with open(csv_file, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    # Support both old "id" and new "cei_id" column names for backward compatibility
                    cei_id = row.get("cei_id", row.get("id", ""))
                    existing_data[cei_id] = {
                        "finding_title": row.get("finding_title", ""),
                        "assessment_id": row.get("assessment_id", "")
                    }
        
        with open(csv_file, 'w', newline='', encoding='utf-8') as f:
            # Column order: cei_id, assessment_id, title, finding_title
            writer = csv.DictWriter(f, fieldnames=["cei_id", "assessment_id", "title", "finding_title"])
            writer.writeheader()
            # Sort by cei_id for consistent output
            sorted_titles = sorted(titles_list, key=lambda x: x["cei_id"])
            for title_row in sorted_titles:
                # Preserve existing finding_title and assessment_id if they exist
                cei_id = title_row["cei_id"]
                if cei_id in existing_data:
                    title_row["finding_title"] = existing_data[cei_id].get("finding_title", "")
                    title_row["assessment_id"] = existing_data[cei_id].get("assessment_id", "")
                writer.writerow(title_row)
        print(f"\nSaved {len(titles_list)} title(s) to '{csv_file}'")
    else:
        print("\nNo titles collected to save.")

def show_menu():
    """Display menu and handle user selection"""
    while True:
        print("\n" + "="*50)
        print("CSV Generation Tool")
        print("="*50)
        print("1. Detect all frameworks")
        print("2. Extract all titles")
        print("3. Exit")
        print("="*50)
        
        choice = input("\nEnter your choice (1-3): ").strip()
        
        if choice == "1":
            detect_frameworks()
        elif choice == "2":
            extract_titles()
        elif choice == "3":
            print("\nExiting...")
            break
        else:
            print("\nInvalid choice. Please enter 1, 2, or 3.")

if __name__ == "__main__":
    show_menu()

