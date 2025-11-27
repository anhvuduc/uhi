import ee
from src.auth import initialize_gee

def check_gee_quota():
    """
    Kiểm tra số lượng task đang chạy/chờ trên GEE.
    Trả về số lượng task để Sensor quyết định.
    """
    try:
        # Đảm bảo đã xác thực
        initialize_gee()
        
        # Lấy danh sách task
        # Lấy danh sách task (không dùng params vì API không hỗ trợ)
        all_tasks = ee.data.getTaskList()
        
        # Lọc client-side
        running_tasks = [t for t in all_tasks if t['state'] in ['READY', 'RUNNING']]
        
        count = len(running_tasks)
        print(f"📊 [QUOTA] Current GEE Tasks (Ready/Running): {count}")
        
        return count
        
    except Exception as e:
        print(f"❌ [ERROR] Failed to check GEE quota: {e}")
        # Trả về 9999 để Sensor tự động sleep (an toàn)
        return 9999
