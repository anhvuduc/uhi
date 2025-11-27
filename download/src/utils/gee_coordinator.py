import ee
import time
from src.auth import initialize_gee

def wait_for_tasks(task_ids):
    """
    Chờ cho đến khi TẤT CẢ các task trong danh sách hoàn thành (COMPLETED) hoặc thất bại (FAILED).
    
    Args:
        task_ids (list): Danh sách Task ID (str) cần theo dõi.
    """
    if not task_ids:
        print("📭 [INFO] Không có task nào cần chờ.")
        return

    print(f"⏳ [WAIT] Bắt đầu theo dõi {len(task_ids)} tasks...")
    initialize_gee()
    
    # Chuyển list thành set để dễ quản lý
    pending_tasks = set(task_ids)
    
    while pending_tasks:
        print(f"    ... Đang chờ {len(pending_tasks)} tasks ...")
        
        # Lấy trạng thái mới nhất của các task đang chờ
        # Lưu ý: getTaskStatus nhận list task ID
        try:
            statuses = ee.data.getTaskStatus(list(pending_tasks))
        except Exception as e:
            print(f"    ⚠️ [WARN] Lỗi khi gọi API getTaskStatus: {e}. Retrying in 60s...")
            time.sleep(60)
            continue

        for status in statuses:
            state = status['state']
            t_id = status['id']
            
            if state == 'COMPLETED':
                print(f"    ✅ [DONE] Task {t_id} completed.")
                pending_tasks.remove(t_id)
            elif state in ['FAILED', 'CANCELLED']:
                print(f"    ❌ [FAIL] Task {t_id} failed/cancelled. Error: {status.get('error_message', 'Unknown')}")
                pending_tasks.remove(t_id)
            # Các trạng thái khác: READY, RUNNING -> Giữ lại trong pending_tasks
            
        if pending_tasks:
            time.sleep(60) # Chờ 60s trước khi check lại
            
    print("🎉 [FINISH] Tất cả task đã kết thúc xử lý.")
