import re
from collections import defaultdict
import os
import time

def read_file_as_list(filename):
    with open(filename, 'r') as file:
        return [line.strip() for line in file.readlines() if line.strip()]

def extract_all_components(lines):
    root_pgs = set()
    child_pgs = set()
    processors = set()
    param_contexts = set()
    param_context_details = defaultdict(dict)
    scheduling_info = defaultdict(dict)

    current_param_context = None
    in_param_section = False
    in_scheduling_section = False
    current_processor_path = None
    current_processor_name = None

    for line_num, line in enumerate(lines):
        # Root PG
        if re.match(r"^\d+\.\s+(.*)\s+\(ID:.*\)", line):
            root_pgs.add(re.sub(r"\(ID:.*", "", line).split(maxsplit=1)[1].strip())
            continue

        # Child PG
        if line.startswith("➔ "):
            match = re.match(r"➔ (.*)\s+\(ID:.*\)", line)
            if match:
                child_pgs.add(match.group(1).strip())
            continue

        # Processor
        proc_match = re.match(r"^- Direct processors inside: \d+ \[(.*)\]", line)
        if proc_match:
            processor_list = proc_match.group(1).split(',')
            for proc in processor_list:
                proc_name = re.sub(r"\(ID:.*\)", "", proc).strip()
                if proc_name and proc_name != "None":
                    processors.add(proc_name)
            continue

        # Parameter Context start
        if line.startswith("Parameter Context Name:"):
            in_param_section = True
            in_scheduling_section = False
            current_param_context = re.sub(r"Parameter Context Name:\s*", "", line).split(" (ID")[0].strip()
            param_contexts.add(current_param_context)
            continue

        # Parameter values
        if in_param_section and line.startswith("- "):
            param_line = line[2:]
            if ':' in param_line:
                key, value = param_line.split(':', 1)
                param_context_details[current_param_context][key.strip()] = value.strip()
            continue

        # Scheduling section start
        if "Below are the Scheduling Info" in line:
            in_scheduling_section = True
            in_param_section = False
            continue

        # Skip divider lines in scheduling section
        if in_scheduling_section and line.startswith("----"):
            continue

        # Scheduling processor path
        if in_scheduling_section and line.startswith("Path: "):
            current_processor_path = line.replace("Path: ", "").strip()
            current_processor_name = None
            continue

        # Scheduling details - more robust parsing
        if in_scheduling_section and current_processor_path and line.strip() and ":" in line:
            # Look for Processor Name
            if line.strip().startswith("Processor Name"):
                current_processor_name = line.split(":", 1)[1].strip()
                if current_processor_path not in scheduling_info:
                    scheduling_info[current_processor_path] = {}
                if current_processor_name not in scheduling_info[current_processor_path]:
                    scheduling_info[current_processor_path][current_processor_name] = {}
            # Look for other scheduling fields when we have a processor name
            elif current_processor_name and not line.startswith("Path:"):
                stripped_line = line.strip()
                if ":" in stripped_line:
                    key_val = stripped_line.split(":", 1)
                    if len(key_val) == 2:
                        key = key_val[0].strip()
                        val = key_val[1].strip()
                        scheduling_info[current_processor_path][current_processor_name][key] = val

    return root_pgs, child_pgs, processors, param_contexts, param_context_details, scheduling_info

def compare_sets(good_set, bad_set):
    return sorted(list(good_set - bad_set))

def write_section(header, items, file_handle):
    file_handle.write(header + "\n")
    if not items:
        file_handle.write("  ✅ No missing items\n\n")
    else:
        for item in items:
            file_handle.write(f"  - {item}\n")
        file_handle.write("\n")

def compare_param_values(good_params, bad_params, file_handle):
    file_handle.write("=== Parameter Context Value Differences ===\n\n")
    diff_found = False

    for context_name in good_params:
        if context_name not in bad_params:
            continue  # already handled as missing context

        good_kv = good_params[context_name]
        bad_kv = bad_params[context_name]

        keys_in_both = set(good_kv.keys()) & set(bad_kv.keys())

        mismatched = []
        for key in keys_in_both:
            if good_kv[key] != bad_kv[key]:
                mismatched.append((key, good_kv[key], bad_kv[key]))

        if mismatched:
            diff_found = True
            file_handle.write(f"Parameter Context: {context_name}\n")
            for key, good_val, bad_val in mismatched:
                file_handle.write(f"  - {key}: Good = {good_val} | Bad = {bad_val}\n")
            file_handle.write("\n")

    if not diff_found:
        file_handle.write("  ✅ No parameter value differences found\n\n")

def compare_scheduling(good_sched, bad_sched, file_handle):
    file_handle.write("=== Scheduling Differences ===\n\n")
    diff_found = False
    scheduling_period_diff_found = False

    # Find processors that exist in both reports
    common_paths = set(good_sched.keys()) & set(bad_sched.keys())

    for path in common_paths:
        path_diff_found = False
        good_procs = good_sched[path]
        bad_procs = bad_sched[path]

        common_procs = set(good_procs.keys()) & set(bad_procs.keys())

        for proc in common_procs:
            proc_diff_found = False
            good_proc_data = good_procs[proc]
            bad_proc_data = bad_procs[proc]

            common_fields = set(good_proc_data.keys()) & set(bad_proc_data.keys())

            differences = []
            for field in common_fields:
                if good_proc_data[field] != bad_proc_data[field]:
                    differences.append((field, good_proc_data[field], bad_proc_data[field]))

            if differences:
                if not path_diff_found:
                    file_handle.write(f"Path: {path}\n")
                    path_diff_found = True
                    diff_found = True

                file_handle.write(f"  Processor: {proc}\n")
                for field, good_val, bad_val in differences:
                    file_handle.write(f"    - {field}: Good = {good_val} | Bad = {bad_val}\n")
                    if field == "Scheduling Period":
                        scheduling_period_diff_found = True
                file_handle.write("\n")

    if not diff_found:
        file_handle.write("  ✅ No scheduling differences found\n\n")
    
    return scheduling_period_diff_found

def has_scheduling_period_differences(good_sched, bad_sched):
    """Check if there are scheduling period differences without writing to file"""
    common_paths = set(good_sched.keys()) & set(bad_sched.keys())
    
    for path in common_paths:
        good_procs = good_sched[path]
        bad_procs = bad_sched[path]
        common_procs = set(good_procs.keys()) & set(bad_procs.keys())
        
        for proc in common_procs:
            good_proc_data = good_procs[proc]
            bad_proc_data = bad_procs[proc]
            
            if "Scheduling Period" in good_proc_data and "Scheduling Period" in bad_proc_data:
                if good_proc_data["Scheduling Period"] != bad_proc_data["Scheduling Period"]:
                    return True
    return False

def compare_scheduling_period_only(good_sched, bad_sched, file_handle):
    file_handle.write("=== Scheduling Period Differences Summary ===\n\n")
    diff_found = False

    # Find processors that exist in both reports
    common_paths = set(good_sched.keys()) & set(bad_sched.keys())

    for path in common_paths:
        good_procs = good_sched[path]
        bad_procs = bad_sched[path]

        common_procs = set(good_procs.keys()) & set(bad_procs.keys())

        path_diff_found = False
        for proc in common_procs:
            good_proc_data = good_procs[proc]
            bad_proc_data = bad_procs[proc]

            # Check if both have Scheduling Period
            if "Scheduling Period" in good_proc_data and "Scheduling Period" in bad_proc_data:
                good_period = good_proc_data["Scheduling Period"]
                bad_period = bad_proc_data["Scheduling Period"]
                
                if good_period != bad_period:
                    if not path_diff_found:
                        file_handle.write(f"Path: {path}\n")
                        path_diff_found = True
                        diff_found = True
                    
                    file_handle.write(f"  Processor: {proc}\n")
                    file_handle.write(f"    - Scheduling Period: Post-validation = {good_period} | Pre-validation = {bad_period}\n")
                    file_handle.write("\n")

    if not diff_found:
        file_handle.write("  ✅ No scheduling period differences found\n\n")

def list_files_with_prefix(directory, prefix):
    try:
        files = os.listdir(directory)
        return sorted([f for f in files if f.startswith(prefix) and (f.endswith(".txt") or f.endswith(".archive"))])
    except FileNotFoundError:
        return []

def prompt_user_to_choose_file(files, file_type):
    print(f"\n📂 Below are the {file_type} Reports found:")
    if not files:
        print("❌ No files found.")
        return None

    for i, file in enumerate(files, 1):
        print(f"{i}. {file}")

    while True:
        choice = input(f"Select {file_type} file (1-{len(files)}): ").strip()
        if choice.isdigit() and 1 <= int(choice) <= len(files):
            return files[int(choice) - 1]
        else:
            print("⚠️ Invalid choice. Please enter a valid number.")

def ensure_reports_directory():
    """Ensure the Reports directory exists, create if it doesn't"""
    reports_dir = "Reports"
    if not os.path.exists(reports_dir):
        os.makedirs(reports_dir)
    return reports_dir

def debug_scheduling_info(scheduling_info, file_type):
    """Debug function to print scheduling information"""
    print(f"\n=== DEBUG: {file_type} Scheduling Info ===")
    for path, processors in scheduling_info.items():
        print(f"Path: {path}")
        for proc_name, proc_data in processors.items():
            print(f"  Processor: {proc_name}")
            for key, value in proc_data.items():
                print(f"    {key}: {value}")
            print()
    print("=" * 50)

def main():
    print("=== NiFi Pre vs Post Environment Comparison ===\n")

    reports_dir = ensure_reports_directory()
    post_files = list_files_with_prefix(reports_dir, "Nifi_Post_Validation_Report_")
    pre_files = list_files_with_prefix(reports_dir, "Nifi_Pre_Validation_Report_")

    good_file = prompt_user_to_choose_file(post_files, "Post-validation")
    if not good_file:
        return

    bad_file = prompt_user_to_choose_file(pre_files, "Pre-validation")
    if not bad_file:
        return

    try:
        good_lines = read_file_as_list(os.path.join(reports_dir, good_file))
        bad_lines = read_file_as_list(os.path.join(reports_dir, bad_file))

        good_root, good_child, good_proc, good_param_names, good_param_kvs, good_sched = extract_all_components(good_lines)
        bad_root, bad_child, bad_proc, bad_param_names, bad_param_kvs, bad_sched = extract_all_components(bad_lines)

        # Debug output - uncomment these lines to see what's being parsed
        # debug_scheduling_info(good_sched, "POST")
        # debug_scheduling_info(bad_sched, "PRE")

        root_diff = compare_sets(good_root, bad_root)
        child_diff = compare_sets(good_child, bad_child)
        proc_diff = compare_sets(good_proc, bad_proc)
        param_diff = compare_sets(good_param_names, bad_param_names)

        report_path = os.path.join(reports_dir, "comparison_report.txt")
        with open(report_path, "w") as report_file:
            report_file.write("=== NiFi Pre vs Post Environment Validation Report ===\n\n")
            write_section(f"Total Root Process Groups difference: {len(root_diff)}", root_diff, report_file)
            write_section(f"Total Child Process Groups difference: {len(child_diff)}", child_diff, report_file)
            write_section(f"Total Processors difference: {len(proc_diff)}", proc_diff, report_file)
            write_section(f"Total Parameter Contexts difference: {len(param_diff)}", param_diff, report_file)
            compare_param_values(good_param_kvs, bad_param_kvs, report_file)
            
            # Scheduling period differences only
            scheduling_period_diff = has_scheduling_period_differences(good_sched, bad_sched)
            compare_scheduling_period_only(good_sched, bad_sched, report_file)
            
            # Summary section
            report_file.write("=== Summary ===\n")
            report_file.write(f"Root Process Groups missing: {len(root_diff)}\n")
            report_file.write(f"Child Process Groups missing: {len(child_diff)}\n")
            report_file.write(f"Processors missing: {len(proc_diff)}\n")
            report_file.write(f"Parameter Contexts missing: {len(param_diff)}\n")
            if scheduling_period_diff:
                report_file.write("⚠️ Scheduling Period differences found - see detailed sections above\n")
            else:
                report_file.write("✅ No Scheduling Period differences found\n")

        print("Comparing Reports .....")
        time.sleep(3)
        print("\n✅ Comparison completed successfully")
        print(f"📄 Report saved to '{report_path}'")

    except FileNotFoundError as fe:
        print(f"❌ File not found: {fe.filename}")
    except Exception as e:
        print(f"❌ Error while comparing files: {e}")

if __name__ == "__main__":
    main()
