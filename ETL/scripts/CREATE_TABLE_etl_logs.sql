CREATE TABLE etl_logs (
    id INT PRIMARY KEY IDENTITY(1,1),
    created_at DATETIME DEFAULT GETDATE(),
    level NVARCHAR(10),
    message NVARCHAR(MAX),
    logger_name NVARCHAR(50),
    func_name NVARCHAR(50),
    line_no INT
);
