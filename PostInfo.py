# --- No changes in import and config sections ---
import requests
import urllib3
import datetime
from io import StringIO
import sys
import os
import subprocess
import re
import time

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

####### Gether Nifi Info
def get_nifi_host_ip():
    try:
        result = subprocess.check_output("kubectl get pod -o wide | grep nifi", shell=True, text=True)
        # IP is usually in the 6th column (e.g., 10.233.85.23)
        ip_match = re.search(r'(\d+\.\d+\.\d+\.\d+)', result)
        return ip_match.group(1) if ip_match else None
    except subprocess.CalledProcessError as e:
        print("Error fetching Nifi pod info:", e)
        return None

def get_nifi_api_ip():
    try:
        result = subprocess.check_output("kubectl get ep | grep kuberiq-vip", shell=True, text=True)
        # Extract the IP from the endpoint info (e.g., 172.29.144.169:80)
        ip_match = re.search(r'(\d+\.\d+\.\d+\.\d+):\d+', result)
        return ip_match.group(1) if ip_match else None
    except subprocess.CalledProcessError as e:
        print("Error fetching Nifi API endpoint:", e)
        return None

# Get the IPs
Nifi_Host = get_nifi_host_ip()
Nifi_Api = get_nifi_api_ip()

#print("Nifi_Host:", Nifi_Host)
#print("Nifi_Api:", Nifi_Api)
#

#####

nifi_api_host = f"https://{Nifi_Api}"
token_url = f"http://{Nifi_Host}:8080/nifi-api/access/token"
username = "radcom"
password = "Radmin@12345"

def get_token():
    credentials = {"username": username, "password": password}
    headers = {"Content-Type": "application/x-www-form-urlencoded"}
    response = requests.post(token_url, data=credentials, headers=headers, verify=False)
    if response.status_code in [200, 201]:
        return response.text
    else:
        raise Exception(f"Failed to get token: {response.status_code} - {response.text}")

def get_root_process_groups(token):
    url = f"{nifi_api_host}/nifi-api/process-groups/root/process-groups"
    headers = {"Accept": "application/json", "Authorization": f"Bearer {token}"}
    response = requests.get(url, headers=headers, verify=False)
    if response.status_code == 200:
        return response.json().get('processGroups', [])
    else:
        raise Exception(f"Failed to get process groups: {response.status_code} - {response.text}")

def get_pg_info(token, pg_id, pg_name=None):
    url = f"{nifi_api_host}/nifi-api/flow/process-groups/{pg_id}"
    headers = {"Accept": "application/json", "Authorization": f"Bearer {token}"}
    response = requests.get(url, headers=headers, verify=False)
    if response.status_code != 200:
        raise Exception(f"Failed to get details for Process Group {pg_id}: {response.status_code} - {response.text}")

    data = response.json()
    flow = data['processGroupFlow']['flow']

    processors = [{
        'id': proc['component']['id'],
        'name': proc['component']['name'],
        'type': proc['component']['type']
    } for proc in flow.get('processors', [])]

    child_groups = []
    for child in flow.get('processGroups', []):
        child_info = get_pg_info(token, child['component']['id'], pg_name=child['component']['name'])
        child_groups.append(child_info)

    total_processors = len(processors) + sum(child.get('total_processors', 0) for child in child_groups)

    return {
        'id': pg_id,
        'name': pg_name if pg_name else "Unknown Group",
        'direct_processors': processors,
        'child_groups': child_groups,
        'total_processors': total_processors
    }

def get_processor_config(token, processor_id):
    url = f"{nifi_api_host}/nifi-api/processors/{processor_id}"
    headers = {"Accept": "application/json", "Authorization": f"Bearer {token}"}
    response = requests.get(url, headers=headers, verify=False)
    if response.status_code == 200:
        return response.json()
    else:
        return None

def find_execute_sql_processors(pg_info, token, path="Root", results=None):
    if results is None:
        results = []

    current_path = f"{path} > {pg_info['name']}"
    for proc in pg_info['direct_processors']:
        if "ExecuteSQL" in proc['type']:
            config = get_processor_config(token, proc['id'])
            if config:
                props = config['component']['config']['properties']
                sql_pre_query = props.get("sql-pre-query", "Not Set")
                sql_post_query = props.get("sql-post-query", "Not Set")
                results.append({
                    "path": current_path,
                    "processor_name": proc['name'],
                    "processor_id": proc['id'],
                    "sql_pre_query": sql_pre_query,
                    "sql_post_query": sql_post_query
                })

    for child in pg_info['child_groups']:
        find_execute_sql_processors(child, token, current_path, results)

    return results

def collect_all_processors_scheduling(pg_info, token, path="Root", results=None):
    """Collect scheduling information for all processors in the process group hierarchy"""
    if results is None:
        results = []

    current_path = f"{path} > {pg_info['name']}"

    # Process direct processors in current group
    for proc in pg_info['direct_processors']:
        config = get_processor_config(token, proc['id'])
        if config:
            scheduling_config = config['component']['config']['schedulingPeriod']
            concurrent_tasks = config['component']['config']['concurrentlySchedulableTaskCount']
            scheduling_strategy = config['component']['config']['schedulingStrategy']
            execution_node = config['component']['config']['executionNode']
            run_duration_millis = config['component']['config']['runDurationMillis']

            results.append({
                "path": current_path,
                "processor_name": proc['name'],
                "processor_id": proc['id'],
                "processor_type": proc['type'],
                "scheduling_period": scheduling_config,
                "concurrent_tasks": concurrent_tasks,
                "scheduling_strategy": scheduling_strategy,
                "execution_node": execution_node,
                "run_duration_millis": run_duration_millis
            })

    # Recursively process child groups
    for child in pg_info['child_groups']:
        collect_all_processors_scheduling(child, token, current_path, results)

    return results

def print_pg_info(pg_info, index=None, indent=0):
    prefix = "   " * indent
    lines = []

    if indent == 0 and index is not None:
        header = f"{index}. {pg_info['name']} (ID: {pg_info['id']})"
    else:
        header = f"{prefix}➔ {pg_info['name']} (ID: {pg_info['id']})"
    lines.append(header)

    direct_proc_count = len(pg_info['direct_processors'])
    if direct_proc_count > 0:
        proc_names = ", ".join([f"{p['name']} (ID: {p['id']})" for p in pg_info['direct_processors']])
    else:
        proc_names = "None"

    lines.append(f"{prefix}   - Total processors inside (including all child groups): {pg_info['total_processors']}")
    lines.append(f"{prefix}   - Direct processors inside: {direct_proc_count} [{proc_names}]")
    lines.append(f"{prefix}   - Number of child process groups inside: {len(pg_info['child_groups'])}")

    for child in pg_info['child_groups']:
        lines.extend(print_pg_info(child, indent=indent+1))

    return lines

def get_root_parameter_context(token):
    url = f"{nifi_api_host}/nifi-api/flow/parameter-contexts"
    headers = {"Accept": "application/json", "Authorization": f"Bearer {token}"}
    response = requests.get(url, headers=headers, verify=False)
    if response.status_code == 200:
        contexts = response.json().get('parameterContexts', [])
        return contexts[0] if contexts else None
    else:
        raise Exception(f"Failed to get parameter contexts: {response.status_code} - {response.text}")

def print_root_parameter_context(context):
    lines = []
    if not context:
        lines.append("\nNo Parameter Contexts found.")
        return lines

    lines.append("\n----------Below are the Parameter Context Info----------------")
    comp = context['component']
    lines.append(f"Parameter Context Name: {comp['name']} (ID: {comp['id']})")

    params = comp.get('parameters', [])
    if params:
        lines.append("Parameters:")
        for param in params:
            name = param['parameter']['name']
            value = param['parameter'].get('value', '')
            lines.append(f"  - {name}: {value}")
    else:
        lines.append("No parameters found in this context.")
    return lines

def print_scheduling_info(scheduling_data):
    """Format scheduling information for output"""
    lines = []
    lines.append("\n--------------Below are the Scheduling Info------------------")

    if not scheduling_data:
        lines.append("✅ No processors found for scheduling information.")
        return lines

    lines.append(f"Total Processors with Scheduling Info: {len(scheduling_data)}")
    lines.append("")

    for item in scheduling_data:
        lines.append(f"Path: {item['path']}")
        lines.append(f"  Processor Name      : {item['processor_name']}")
        lines.append(f"  Processor ID        : {item['processor_id']}")
        lines.append(f"  Processor Type      : {item['processor_type']}")
        lines.append(f"  Scheduling Period   : {item['scheduling_period']}")
        lines.append(f"  Concurrent Tasks    : {item['concurrent_tasks']}")
        lines.append(f"  Scheduling Strategy : {item['scheduling_strategy']}")
        lines.append(f"  Execution Node      : {item['execution_node']}")
        lines.append(f"  Run Duration (ms)   : {item['run_duration_millis']}")
        lines.append("-" * 60)

    return lines

def ensure_reports_directory():
    """Ensure the Reports directory exists, create if it doesn't"""
    reports_dir = "Reports"
    if not os.path.exists(reports_dir):
        os.makedirs(reports_dir)
    return reports_dir

def save_output_to_file(content, is_backup=False):
    reports_dir = ensure_reports_directory()
    now = datetime.datetime.now()
    suffix = now.strftime("%d%m%Y_%H-%M-%S")
    extension = ".archive" if is_backup else ".txt"
    filename = f"Nifi_Post_Validation_Report_{suffix}{extension}"
    filepath = os.path.join(reports_dir, filename)
    with open(filepath, "w", encoding="utf-8") as f:
        f.write(content)
    print(f"\n✅ Report saved to: {filepath}")

def save_detailed_execute_sql(results, is_backup=False):
    reports_dir = ensure_reports_directory()
    now = datetime.datetime.now()
    suffix = now.strftime("%d_%m_%Y_%H-%M-%S")
    extension = ".archive" if is_backup else ".txt"
    filename = f"Nifi_Post_Validation_Detailed_Report_{suffix}{extension}"
    filepath = os.path.join(reports_dir, filename)
    with open(filepath, "w", encoding="utf-8") as f:
        f.write("=== ExecuteSQL Processor SQL Pre/Post-Query Report ===\n\n")
        if not results:
            f.write("✅ No ExecuteSQL processors found.\n")
        for item in results:
            f.write(f"Path: {item['path']}\n")
            f.write(f"  Processor Name  : {item['processor_name']}\n")
            f.write(f"  Processor ID    : {item['processor_id']}\n")
            f.write(f"  SQL Pre-Query   : {item['sql_pre_query']}\n")
            f.write(f"  SQL Post-Query  : {item['sql_post_query']}\n")
            f.write("-" * 60 + "\n")
    print(f"✅ Detailed ExecuteSQL Report saved to: {filepath}")

if __name__ == "__main__":
    try:
        print("Please choose the purpose of Report Generation:")
        print("    1) For Comparison")
        print("    2) For Backup")
        print("    3) For Exit")
        choice = input("Enter your choice (1, 2 or 3): ").strip()
        if choice == "3":
            print("\n➡️ Exiting the program... Goodbye!")
            time.sleep(2)
            sys.exit(0)
        print("Generating Report .....")
        is_backup = choice == "2"

        token = get_token()
        root_process_groups = get_root_process_groups(token)
        total_root = len(root_process_groups)

        output_lines = [f"Total number of process groups at root: {total_root}\n"]
        execute_sql_data = []
        scheduling_data = []

        for idx, pg in enumerate(root_process_groups, start=1):
            pg_id = pg['component']['id']
            pg_name = pg['component']['name']
            pg_info = get_pg_info(token, pg_id, pg_name)

            output_lines.extend(print_pg_info(pg_info, index=idx))
            output_lines.append("")

            execute_sql_data.extend(find_execute_sql_processors(pg_info, token, path="Root"))
            scheduling_data.extend(collect_all_processors_scheduling(pg_info, token, path="Root"))

        # Add parameter context
        root_parameter_context = get_root_parameter_context(token)
        output_lines.extend(print_root_parameter_context(root_parameter_context))

        # Add scheduling information
        output_lines.extend(print_scheduling_info(scheduling_data))

        # Save main report
        full_report = "\n".join(output_lines)
        save_output_to_file(full_report, is_backup=is_backup)

        # Save ExecuteSQL report
        save_detailed_execute_sql(execute_sql_data, is_backup=is_backup)

    except Exception as e:
        print(f"❌ Error: {e}")
