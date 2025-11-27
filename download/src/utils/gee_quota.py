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
        # limit=5000 để đếm được nhiều, nhưng cẩn thận timeout nếu quá nhiều
        tasks = ee.data.getTaskList(params={'state': ['READY', 'RUNNING']})
        
        count = len(tasks)
        print(f"📊 [QUOTA] Current GEE Tasks (Ready/Running): {count}")
        
        return count
        
    except Exception as e:
        print(f"❌ [ERROR] Failed to check GEE quota: {e}")
        # Trả về 9999 để Sensor tự động sleep (an toàn)
        return 9999
