import ee
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from src.auth import initialize_gee

def get_satellite_dates(collection_id):
    """
    Lấy ngày bắt đầu và kết thúc của một ImageCollection từ GEE.
    
    Args:
        collection_id (str): ID của bộ dữ liệu (VD: 'MODIS/061/MOD11A1')
        
    Returns:
        tuple: (start_date_str, end_date_str) định dạng 'YYYY-MM-DD'
    """
    try:
        initialize_gee()
        
        # Lấy metadata của collection
        # Cách tối ưu: Lấy image đầu và cuối
        col = ee.ImageCollection(collection_id)
        
        # Lấy ngày sớm nhất
        first_img = col.sort('system:time_start', True).first()
        start_date_ms = first_img.get('system:time_start').getInfo()
        
        # Lấy ngày muộn nhất
        last_img = col.sort('system:time_start', False).first()
        end_date_ms = last_img.get('system:time_start').getInfo()
        
        if start_date_ms and end_date_ms:
            start_date = datetime.fromtimestamp(start_date_ms / 1000).strftime('%Y-%m-%d')
            end_date = datetime.fromtimestamp(end_date_ms / 1000).strftime('%Y-%m-%d')
            return start_date, end_date
            
    except Exception as e:
        print(f"⚠️ [WARN] Không thể lấy date range cho {collection_id}: {e}")
        
    # Fallback nếu lỗi (hoặc trả về None để xử lý sau)
    return '2000-01-01', datetime.now().strftime('%Y-%m-%d')

def get_date_range(mode, params=None):
    """
    Tính toán start_date và end_date dựa trên Mode chạy.
    
    Args:
        mode (str): 'yearly', 'historical', 'custom'
        params (dict): Các tham số từ Airflow (dag_run.conf)
        
    Returns:
        tuple: (start_date, end_date) dạng 'YYYY-MM-DD'
    """
    today = datetime.now()
    
    if mode == 'yearly':
        # Mặc định: Chạy cho năm ngoái
        last_year = today.year - 1
        start_date = f"{last_year}-01-01"
        end_date = f"{last_year+1}-01-01"
        return start_date, end_date
        
    elif mode == 'custom':
        # Lấy từ params
        if params and 'start_date' in params and 'end_date' in params:
            return params['start_date'], params['end_date']
        else:
            # Fallback nếu không nhập
            return today.strftime('%Y-%m-%d'), today.strftime('%Y-%m-%d')
            
    elif mode == 'historical':
        # Mode này đặc biệt: Sẽ trả về 'HISTORICAL' để logic chính tự gọi get_satellite_dates
        return 'HISTORICAL', 'HISTORICAL'
        
    return None, None

def generate_date_chunks(start_date, end_date):
    """
    Generator chia nhỏ khoảng thời gian thành các chunk (mặc định theo tháng).
    Giúp đồng bộ logic giữa bước Estimate và Execution.
    
    Yields:
        tuple: (chunk_start_str, chunk_end_str)
    """
    start_dt = datetime.strptime(start_date, '%Y-%m-%d')
    end_dt = datetime.strptime(end_date, '%Y-%m-%d')
    current_dt = start_dt
    
    while current_dt < end_dt:
        # Logic: Lấy ngày đầu tháng sau
        next_month = current_dt + timedelta(days=32)
        next_month = next_month.replace(day=1)
        
        chunk_end_dt = min(next_month, end_dt)
        
        chunk_start_str = current_dt.strftime('%Y-%m-%d')
        chunk_end_str = chunk_end_dt.strftime('%Y-%m-%d')
        
        if chunk_start_str == chunk_end_str:
            break
            
        yield chunk_start_str, chunk_end_str
        
        current_dt = chunk_end_dt
