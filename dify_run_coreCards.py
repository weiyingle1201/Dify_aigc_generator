import requests
import json
import pandas as pd
import time
import os
import re

# --- 1. 你的配置 ---
API_URL = "https://difydev.qa.molimoli.co/v1/workflows/run"
API_KEY = "app-oVlb6daqnz3GeMGuWeYS4CBG" # 替换成你的API密钥
CSV_FILE_PATH = "duolinguo_core_cards.csv"         # 确保这是你包含cid列的CSV文件名
OUTPUT_DIR = "results_text_coreCards"

# --- 2. 创建结果文件夹 ---
if not os.path.exists(OUTPUT_DIR):
    os.makedirs(OUTPUT_DIR)

# --- 3. 准备请求头 ---
headers = {
    "Authorization": f"Bearer {API_KEY}",
    "Content-Type": "application/json",
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36"
}

# --- 4. 读取并处理任务 ---
try:
    tasks = pd.read_csv(CSV_FILE_PATH).to_dict('records')
    print(f"成功读取 {len(tasks)} 个主题任务，准备开始执行...")
    print("-" * 50)

    # --- 5. 循环调用API ---
    for i, task in enumerate(tasks):
        topic = task["topic"]
        level = task["level"]
        cid = task["cid"] # <-- 新增：从task字典中读取cid
        
        # 更新打印信息，显示cid
        print(f"--- 正在执行任务 {i+1}/{len(tasks)}: Topic = {topic}, CID = {cid} ---")
        
        payload = {
            "inputs": {
                "topic": topic,
                "level": str(level).split(' ')[-1],
                "num_questions_type_1": 1, "num_questions_type_2": 1, "num_questions_type_3": 1,
                "num_questions_type_4": 1, "num_questions_type_5": 1, "num_questions_type_6": 1,
                "num_questions_type_7": 1, "num_questions_type_8": 1, "num_questions_type_9": 1,
                "cid": str(cid) # <-- 新增：将cid添加到API请求的inputs中
            },
            "response_mode": "streaming",
            "user": f"final_batch_user_{i}"
        }

        try:
            # (这部分代码和之前一样)
            response = requests.post(API_URL, headers=headers, json=payload, timeout=30, verify=False, stream=True)
            response.raise_for_status()

            final_output = None
            for line in response.iter_lines():
                if line and line.strip().startswith(b'data:'):
                    line_str = line.decode('utf-8')
                    json_part = line_str[len('data:'):].strip()
                    if json_part:
                        try:
                            event_data = json.loads(json_part)
                            if event_data.get("event") == "workflow_finished":
                                final_output = event_data.get("data", {}).get("outputs", {})
                                break
                        except json.JSONDecodeError:
                            continue
            
            if final_output:
                print("任务成功！")
                safe_topic_name = re.sub(r'[\\/*?:"<>|]', "", str(task['topic'])).replace(' ', '_')
                file_name = f"{OUTPUT_DIR}/{i+1:02d}_{safe_topic_name}.json"
                with open(file_name, "w", encoding="utf-8") as f:
                    json.dump(final_output, f, indent=2, ensure_ascii=False)
                print(f"最终结果已保存到 {file_name}")
            else:
                print("任务执行结束，但未能从数据流中提取到最终结果。")

        except requests.exceptions.RequestException as e:
            print(f"任务执行失败: {e}")
            if e.response:
                print(f"错误详情: {e.response.text}")


        print("-" * 50)
        time.sleep(1)

    print("所有任务执行完毕！")

except FileNotFoundError:
    print(f"错误：找不到任务文件 {CSV_FILE_PATH}。")
except Exception as e:
    print(f"发生未知错误: {e}")