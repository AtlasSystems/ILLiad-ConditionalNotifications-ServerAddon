luanet.load_assembly("log4net");

local types = {}
types["log4net.LogManager"] = luanet.import_type("log4net.LogManager");
local log = types["log4net.LogManager"].GetLogger("AtlasSystems.Addons.ConditionalNotifications");

local Settings = {};
Settings.NVTGC = GetSetting("NVTGC");
Settings.ArticleEmailName = GetSetting("ArticleEmailName");
Settings.LoanEmailName = GetSetting("LoanEmailName");

local isCurrentlyProcessing = false;
local sharedServerSupport = false;

function Init()
    log:Debug("Initializing Conditional Notifications addon.");
    InitializeSettings();
    RegisterSystemEventHandler("SystemTimerElapsed", "TimerElapsed");
end

function InitializeSettings();
    -- Initialize settings to be usable in SQL queries.
    if Settings.NVTGC:find("%w") then
        Settings.NVTGC = "'" .. Settings.NVTGC:gsub("%s*,%s*", ","):gsub(",", "','") .. "'";
    end
end

function TimerElapsed()
    if not Settings.ArticleEmailName or Settings.ArticleEmailName == "" or not Settings.LoanEmailName or Settings.LoanEmailName == "" then
        log:Warn("One or both e-mail name settings is blank. Please ensure both ArticleEmailName and LoanEmailName are valid notification template names.");
        return;
    end

    if not isCurrentlyProcessing then
        local connection = CreateManagedDatabaseConnection();

        local success, transactionNumbersOrErr = pcall(function()
            connection:Connect();

            SetSharedServerSupport(connection);
            local usersTable = "Users";
            if sharedServerSupport then
                usersTable = "UsersALL";
            end
            
            -- Check for history entry to ensure request is at the right status for the borrower, or cut down on delay before it us updated for ILLiad users.
            local queryString = [[SELECT Transactions.TransactionNumber FROM Transactions 
            INNER JOIN ]] .. usersTable .. [[ ON Transactions.Username = ]] .. usersTable .. [[.Username
            INNER JOIN History ON Transactions.TransactionNumber = History.TransactionNumber
            WHERE TransactionStatus = 'Request Conditionalized' 
            AND Entry LIKE 'Updated on OCLC as Conditional%']];

            if Settings.NVTGC:find("%w") then
                queryString = queryString .. " AND NVTGC IN(" .. Settings.NVTGC .. ")";
            end

            connection.QueryString = queryString;
            LogDatabaseQuery(queryString);
            local queryResults = connection:Execute();

            local transactionNumbers = {};
            if (queryResults.Rows.Count > 0) then
                for i = 0, queryResults.Rows.Count - 1 do
                    transactionNumbers[#transactionNumbers+1] = queryResults.Rows:get_Item(i):get_Item("TransactionNumber");
                end
            end

            return transactionNumbers;
        end);

        connection:Dispose();

        if not success then
            log:Error("An error occurred when retrieving transaction info from the database: " .. tostring(TraverseError(transactionNumbersOrErr)));
            return;
        end

        if #transactionNumbersOrErr > 0 then
            ProcessDataContexts("TransactionNumber", transactionNumbersOrErr, "SendConditionalNotifications");
        end

        isCurrentlyProcessing = false;
    else
        log:Debug("Still processing requests for conditional notifications.");
    end
end

function SendConditionalNotifications()
    local transactionNumber = GetFieldValue("Transaction", "TransactionNumber");
    
    local templateName;
    if GetFieldValue("Transaction", "RequestType") == "Article" then
        templateName = Settings.ArticleEmailName;
    else
        templateName = Settings.LoanEmailName;
    end

    local emailSent = HasEmailBeenSent(transactionNumber, templateName);
    if emailSent == true then
        log:Debug("Conditional notification has already been sent for transaction " .. transactionNumber .. ".");
        return;
    elseif emailSent == "error" then
        -- Error has already been logged at this point.
        return;
    end

    log:Debug("Sending conditional notification for transaction " .. transactionNumber .. ".");

    ExecuteCommand("SendTransactionNotification", {transactionNumber, templateName});
end

function HasEmailBeenSent(transactionNumber, templateName)
    local connection = CreateManagedDatabaseConnection();
    local success, emailSubjectOrErr = pcall(function()
        connection:Connect();

        local queryString = "SELECT Subject FROM NotificationTemplates WHERE Name = '" .. templateName .. "'";
        connection.QueryString = queryString;
        LogDatabaseQuery(queryString);

        return connection:ExecuteScalar();
    end);

    connection:Dispose();

    if not success then
        log:Error("An error occurred when retrieving notification template info from the database: " .. tostring(TraverseError(emailSubjectOrErr)));
        return "error";
    end

    -- Replace template tags with SQL wildcards and escape quotes.
    local wildcardedSubject = emailSubjectOrErr:gsub("<#.->", "%%"):gsub("'", "''");

    local connection = CreateManagedDatabaseConnection();
    local success, subjectCountOrErr = pcall(function()
        connection:Connect();

        local queryString = "SELECT COUNT(*) FROM EMailCopies WHERE TransactionNumber = " .. transactionNumber .. " AND Subject LIKE '" .. wildcardedSubject .. "'";
        connection.QueryString = queryString;
        LogDatabaseQuery(queryString);
        
        return connection:ExecuteScalar();
    end);

    connection:Dispose();

    if not success then
        log:Error("An error occurred when retrieving e-mail history info from the database: " .. tostring(TraverseError(subjectCountOrErr)));
        return "error";
    end

    if subjectCountOrErr == 0 then
        return false;
    else
        return true;
    end
end

function LogDatabaseQuery(queryString)
    log:Debug("Querying the database with querystring: " .. queryString);
end

function SetSharedServerSupport(connection)
    connection.QueryString = "SELECT Value FROM Customization WHERE CustKey = 'SharedServerSupport' AND NVTGC = 'ILL'";
    local value = connection:ExecuteScalar();

    if value == "Yes" then
        log:Debug("Shared Server Support enabled");
        sharedServerSupport = true;
    else
        log:Debug("Shared Server Support not enabled");
        sharedServerSupport = false;
    end
end

function TraverseError(e)
    if not e.GetType then
        -- Not a .NET type
        return e;
    else
        if not e.Message then
            -- Not a .NET exception
            return e;
        end
    end

    log:Debug(e.Message);

    if e.InnerException then
        return TraverseError(e.InnerException);
    else
        return e.Message;
    end
end

function OnError(err)
    -- To ensure the addon doesn't get stuck in processing if it encounters an error.
    isCurrentlyProcessing = false;
    log:Error(tostring(TraverseError(err)));
end