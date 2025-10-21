#采集专业详情页数据
import requests
import bson
import pymysql
from datetime import datetime
import time


def main():
    headers = {
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36 Edg/141.0.0.0"
    }
    url = "https://interactive.guim.co.uk/atoms/labs/2024/09/university-guide/overview/v/1755081891846/assets/data/overview.json"

    # 数据库连接配置
    #这里进行你的数据库配置比如
    conn = ()

    try:
        cursor = conn.cursor()

        # 获取响应数据
        response = requests.get(url, headers=headers)
        page_json = response.json()

        total_subjects = len(page_json['subjects'])
        total_inserted = 0

        print(f"开始处理 {total_subjects} 个专业的数据...")

        for subject_index, subject in enumerate(page_json['subjects'], 1):
            subject_id = subject['id']
            title = subject['title']

            print(f"\n处理专业 {subject_index}/{total_subjects}: {title}")

            # 先查询guardian_id
            guardian_id = ''
            try:
                cursor.execute(
                    "SELECT id FROM school_major WHERE major_ename = %s AND year_time = 2025",
                    (title,)
                )
                result = cursor.fetchone()
                if result:
                    guardian_id = result[0]
                    print(f"找到匹配的guardian_id: {guardian_id}")
                else:
                    print(f"未找到匹配的major_ename: {title}")
            except Exception as e:
                print(f"查询guardian_id失败: {str(e)}")

            # 获取专业详情数据
            detail_url = f"https://interactive.guim.co.uk/atoms/labs/2024/09/university-guide/overview/v/1755081891846/assets/data/{subject_id}.json"
            try:
                response1 = requests.get(url=detail_url, headers=headers)
                response1.encoding = 'utf-8'
                pg_json = response1.json()

                subject_insert_count = 0
                for institution in pg_json.get('institutions', []):
                    # 生成唯一ID
                    id = str(bson.objectid.ObjectId())

                    # 构建插入数据
                    data = (
                        id,
                        2025,  # year_time
                        institution.get('rank', ''),
                        guardian_id,
                        datetime.now(),  # update_date_time
                        institution.get('name', ''),
                        '',  # school_cname
                        '',  # school_id
                        '',  # school_major_id
                        institution.get('guardianScore', ''),
                        '',  # satisfied_with_course
                        institution.get('percentSatisfiedWithTeaching', ''),
                        institution.get('percentSatisfiedWithAssessment', ''),
                        institution.get('studentStaffRatio', ''),
                        institution.get('expenditurePerStudent', ''),
                        institution.get('averageEntryTariff', ''),
                        institution.get('valueAdded', ''),
                        institution.get('careerProspects', ''),
                        institution.get('continuation', ''),
                        '',  # country_id
                        ''  # school_major_cid
                    )

                    # 执行插入
                    try:
                        cursor.execute("""
                            INSERT INTO school 
                            (id, year_time, ranking, guardian_id, update_date_time, 
                            school_ename, school_cname, school_id, school_major_id, 
                            guardian_score, satisfied_with_course, satisfied_with_teaching, 
                            satisfied_with_feedback, student_staff_ratio, spend_per_student, 
                            average_entry_tariff, value_added_score, career_after_mths, 
                            continuation, country_id, school_major_cid) 
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """, data)
                        subject_insert_count += 1
                        total_inserted += 1
                    except Exception as e:
                        print(f"插入数据失败: {str(e)}")
                        continue

                print(f"成功插入 {subject_insert_count} 条数据")

                # 每处理一个专业提交一次事务
                conn.commit()

                # 避免请求过于频繁
                time.sleep(1)

            except Exception as e:
                print(f"获取专业详情失败: {str(e)}")
                conn.rollback()
                continue

        print(f"\n数据处理完成！总共插入 {total_inserted} 条记录")

    except Exception as e:
        print(f"发生错误: {str(e)}")
        conn.rollback()
    finally:
        if 'cursor' in locals():
            cursor.close()
        if conn:
            conn.close()


if __name__ == "__main__":
    main()

