import subprocess
import re

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

print("Nifi_Host:", Nifi_Host)
print("Nifi_Api:", Nifi_Api)

