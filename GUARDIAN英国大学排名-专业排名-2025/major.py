import requests
import bson
from datetime import datetime
import pymysql
import time

headers = {
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36 Edg/141.0.0.0"
}
url = "https://interactive.guim.co.uk/atoms/labs/2024/09/university-guide/overview/v/1755081891846/assets/data/overview.json"

# 数据库连接配置
#此处进行你自己的数据库配置比如：
conn = ()


def main():
    print("开始导入Guardian专业排名数据...")
    success_count = 0
    fail_count = 0

    try:
        # 获取数据
        response = requests.get(url, headers=headers)
        page_json = response.json()

        with conn.cursor() as cursor:
            # 遍历每个专业数据
            subjects = page_json.get('subjects', [])
            total = len(subjects)
            for idx, subject in enumerate(subjects, 1):
                try:
                    # 准备数据 - 严格按照要求的字段顺序
                    id_value = str(bson.ObjectId())
                    major_ename = subject['title']
                    major_cname = ''
                    year_time = 2025
                    major_url = 'https://www.theguardian.com/education/ng-interactive/2024/sep/07/the-guardian-university-guide-2025-the-rankings'
                    major_classify_id = ''

                    # 构建插入SQL，确保字段顺序与数据表一致
                    sql = """INSERT INTO school
                           (id, major_ename, major_cname, year_time, major_url, major_classify_id)
                           VALUES (%s, %s, %s, %s, %s, %s)"""

                    # 执行插入，参数顺序必须与SQL中字段顺序完全一致
                    cursor.execute(sql, (id_value, major_ename, major_cname, year_time, major_url, major_classify_id))
                    success_count += 1

                    # 每10条数据提交一次
                    if idx % 10 == 0 or idx == total:
                        conn.commit()
                        print(f"已处理 {idx}/{total} 条数据，成功: {success_count}, 失败: {fail_count}")

                except Exception as e:
                    fail_count += 1
                    print(f"处理第 {idx} 条数据失败: {str(e)}")
                    # 跳过当前记录继续处理
                    continue

        # 最终提交
        conn.commit()
        print(f"\n数据导入完成!")
        print(f"总处理: {total} 条")
        print(f"成功导入: {success_count} 条")
        print(f"导入失败: {fail_count} 条")

    except Exception as e:
        print(f"数据库操作失败: {str(e)}")
        conn.rollback()
    finally:
        conn.close()


if __name__ == "__main__":
    start_time = time.time()
    main()
    end_time = time.time()
    print(f"\n程序运行时间: {end_time - start_time:.2f} 秒")







