
CREATE TABLE dbo.Gold_Social_Users (
    id INT PRIMARY KEY,                       -- Exact match for root 'id'
    name VARCHAR(150) NULL,               -- Exact match for root 'name'
    username VARCHAR(100) NULL,           -- Exact match for root 'username'
    email VARCHAR(255) NULL,              -- Exact match for root 'email'
    phone VARCHAR(50) NULL,               -- Exact match for root 'phone'
    website VARCHAR(100) NULL,            -- Exact match for root 'website'
    street VARCHAR(150) NULL,             -- Flattened: address.street -> street
    suite VARCHAR(50) NULL,               -- Flattened: address.suite -> suite
    city VARCHAR(100) NULL,               -- Flattened: address.city -> city
    zipcode VARCHAR(50) NULL,             -- Flattened: address.zipcode -> zipcode
    geoLat DECIMAL(9,6) NULL,             -- Flattened: address.geo.lat -> geoLat
    geoLng DECIMAL(9,6) NULL,             -- Flattened: address.geo.lng -> geoLng
    companyName VARCHAR(150) NULL,        -- Flattened: company.name -> companyName
    companyCatchPhrase VARCHAR(255) NULL, -- Flattened: company.catchPhrase -> companyCatchPhrase
    companyBs VARCHAR(255) NULL,          -- Flattened: company.bs -> companyBs
    IngestionTimestamp DATETIME DEFAULT GETDATE()
);

CREATE TABLE dbo.Gold_Social_Posts (
    PostID INT PRIMARY KEY,
    UserID INT NOT NULL, -- Logical Foreign Key linking to dbo.Gold_Social_Users
    Title VARCHAR(255) NOT NULL,
    Body VARCHAR(MAX) NOT NULL,
    IngestionTimestamp DATETIME DEFAULT GETDATE()
);

CREATE TABLE dbo.Gold_Social_Comments (
    CommentID INT PRIMARY KEY,
    PostID INT NOT NULL, -- Logical Foreign Key linking to dbo.Gold_Social_Posts
    CommenterName VARCHAR(150) NOT NULL,
    CommenterEmail VARCHAR(255) NOT NULL,
    Body VARCHAR(MAX) NOT NULL,
    IngestionTimestamp DATETIME DEFAULT GETDATE()
);

CREATE TABLE dbo.Gold_Social_Albums (
    AlbumID INT PRIMARY KEY,
    UserID INT NOT NULL,
    Title VARCHAR(255) NOT NULL,
    IngestionTimestamp DATETIME DEFAULT GETDATE() 
);
CREATE TABLE dbo.Gold_Social_Photos (
    PhotoID INT PRIMARY KEY,
    AlbumID INT NOT NULL, -- Logical Foreign Key linking to dbo.Gold_Social_Albums
    Title VARCHAR(255) NOT NULL,
    SourceURL VARCHAR(500) NOT NULL,     -- Maps to 'url'
    ThumbnailURL VARCHAR(500) NOT NULL,  -- Maps to 'thumbnailUrl'
    IngestionTimestamp DATETIME DEFAULT GETDATE()
);
CREATE TABLE dbo.Gold_Social_Todos (
    TodoID INT PRIMARY KEY,
    UserID INT NOT NULL,
    Title VARCHAR(255) NOT NULL,
    IsCompleted BIT NOT NULL, -- Maps to 'completed' (0 = False, 1 = True)
    IngestionTimestamp DATETIME DEFAULT GETDATE()
);