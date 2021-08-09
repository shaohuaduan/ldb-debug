package main

import (
    "fmt"
    "os"
    "bufio"
    "strings"
    "strconv"
    "time"
	//"log"
	badger "github.com/dgraph-io/badger"
    options "github.com/dgraph-io/badger/options"
)

func Opendb(cmdSplits []string) *badger.DB {
    //fmt.Println("Invoke opendb")
    var opt badger.Options
    //opt := badger.DefaultOptions("/tmp/badger")///tmp/badger
    cmdTokenIndex := 0
    for i := 0; i < len(cmdSplits); i++ {

        if !strings.HasPrefix(cmdSplits[i], "--") {
            //fmt.Printf("Ldb Command name: %s \n", cmdSplits[i])
                    
            if cmdTokenIndex == 1 {
                //fmt.Printf("Ldb Command key: %s \n", cmdSplits[i])
                cmdTokenIndex++
                opt = badger.DefaultOptions(cmdSplits[i]).WithLogger(nil)
                // opt.WithLoggingLevel(defaultLogger(ERROR))
                break
            } 
            cmdTokenIndex++
        }
    }

    for i := 0; i < len(cmdSplits); i++ {
        //fmt.Println(i, cmdSplits[i])
        if strings.HasPrefix(cmdSplits[i], "--") && strings.Contains(cmdSplits[i], "="){
            //fmt.Println("Ldb Command option: ", strings.TrimPrefix(cmdSplits[i], "--"))
            optionString := strings.TrimPrefix(cmdSplits[i], "--")
            optionArray := strings.Split(optionString, "=")
            //fmt.Printf("option key: %s, value: %s \n", optionArray[0], optionArray[1])
            switch optionArray[0] {
            case "ValueDir": //ValueDir
                opt.WithValueDir(optionArray[1])
            case "SyncWrites": //SyncWrites
                syncwrites, _ := strconv.ParseBool(optionArray[1])
                opt.WithSyncWrites(syncwrites)
            case "NumVersionsToKeep": //NumVersionsToKeep
                numVersionsToKeep, _ := strconv.Atoi(optionArray[1])
                opt.WithNumVersionsToKeep(numVersionsToKeep)
            case "NumGoroutines": //NumGoroutines
                numGoroutines, _ := strconv.Atoi(optionArray[1])
                opt.WithNumGoroutines(numGoroutines)
            case "ReadOnly": //ReadOnly
                readOnly, _ := strconv.ParseBool(optionArray[1])
                opt.WithReadOnly(readOnly)
            case "MetricsEnabled": //MetricsEnabled
                metricsEnabled, _ := strconv.ParseBool(optionArray[1])
                opt.WithMetricsEnabled(metricsEnabled)
            case "Logger": //Logger
                switch optionArray[1] {
                    case "DEBUG": //DEBUG
                        opt.WithLoggingLevel(badger.DEBUG)
                    case "INFO": //INFO
                        opt.WithLoggingLevel(badger.INFO)
                    case "WARNING": //WARNING
                        opt.WithLoggingLevel(badger.WARNING)
                    case "ERROR": //ERROR
                        opt.WithLoggingLevel(badger.ERROR)
                    default:
                        fmt.Printf("(Ldb-Tool) Invalid Options: %s\n", cmdSplits[i])
                        return nil
                }
                //opt.WithLogger(badger.defaultLogger(badger.DEBUG))
            case "LoggingLevel": //LoggingLevel
                switch optionArray[1] {
                    case "DEBUG": //DEBUG
                        opt.WithLoggingLevel(badger.DEBUG)
                    case "INFO": //INFO
                        opt.WithLoggingLevel(badger.INFO)
                    case "WARNING": //WARNING
                        opt.WithLoggingLevel(badger.WARNING)
                    case "ERROR": //ERROR
                        opt.WithLoggingLevel(badger.ERROR)
                    default:
                        fmt.Printf("(Ldb-Tool) Invalid Options: %s\n", cmdSplits[i])
                        return nil
            }
            case "BaseTableSize": //BaseTableSize
                baseTableSize, _ := strconv.ParseInt(optionArray[1], 10, 64)
                opt.WithBaseTableSize(baseTableSize)
            case "LevelSizeMultiplier": //LevelSizeMultiplier
                levelSizeMultiplier, _ := strconv.Atoi(optionArray[1])
                opt.WithLevelSizeMultiplier(levelSizeMultiplier)
            case "MaxLevels": //MaxLevels
                maxLevels, _ := strconv.Atoi(optionArray[1])
                opt.WithMaxLevels(maxLevels)
            case "ValueThreshold": //ValueThreshold
                valueThreshold, _ := strconv.ParseInt(optionArray[1], 10, 64)
                opt.WithValueThreshold(valueThreshold)
            case "VLogPercentile": //VLogPercentile
                vLogPercentile, _ := strconv.ParseFloat(optionArray[1], 64)
                opt.WithVLogPercentile(vLogPercentile)
            case "NumMemtables": //NumMemtables
                numMemtables, _ := strconv.Atoi(optionArray[1])
                opt.WithNumMemtables(numMemtables)
            case "MemTableSize": //MemTableSize
                memTableSize, _ := strconv.ParseInt(optionArray[1], 10, 64)
                opt.WithMemTableSize(memTableSize)
            case "BloomFalsePositive": //BloomFalsePositive
                bloomFalsePositive, _ := strconv.ParseFloat(optionArray[1], 64)
                opt.WithBloomFalsePositive(bloomFalsePositive)
            case "BlockSize": //BlockSize
                blockSize, _ := strconv.Atoi(optionArray[1])
                opt.WithBlockSize(blockSize)
            case "NumLevelZeroTables": //NumLevelZeroTables
                numLevelZeroTables, _ := strconv.Atoi(optionArray[1])
                opt.WithNumLevelZeroTables(numLevelZeroTables)
            case "NumLevelZeroTablesStall": //NumLevelZeroTablesStall
                numLevelZeroTablesStall, _ := strconv.Atoi(optionArray[1])
                opt.WithNumLevelZeroTablesStall(numLevelZeroTablesStall)
            case "BaseLevelSize": //BaseLevelSize
                baseLevelSize, _ := strconv.ParseInt(optionArray[1], 10, 64)
                opt.WithBaseLevelSize(baseLevelSize)
            case "ValueLogFileSize": //ValueLogFileSize
                valueLogFileSize, _ := strconv.ParseInt(optionArray[1], 10, 64)
                opt.WithValueLogFileSize(valueLogFileSize)
            case "ValueLogMaxEntries": //ValueLogMaxEntries
                valueLogMaxEntries, _ := strconv.ParseInt(optionArray[1], 10, 32)
                opt.WithValueLogMaxEntries(uint32(valueLogMaxEntries))
            case "NumCompactors": //NumCompactors
                numCompactors, _ := strconv.Atoi(optionArray[1])
                opt.WithNumCompactors(numCompactors)
            case "CompactL0OnClose": //CompactL0OnClose
                compactL0OnClose, _ := strconv.ParseBool(optionArray[1])
                opt.WithCompactL0OnClose(compactL0OnClose)
            case "EncryptionKey": //EncryptionKey
                //encryptionkey, _ := 
                opt.WithEncryptionKey([]byte(optionArray[1]))
            case "EncryptionKeyRotationDuration": //EncryptionKeyRotationDuration
                encryptionKeyRotationDuration, _ := strconv.ParseInt(optionArray[1], 10, 64)
                opt.WithEncryptionKeyRotationDuration(time.Duration(encryptionKeyRotationDuration) * time.Hour)
            case "Compression": //Compression
                //opt.WithCompression(optionArray[1])
                switch optionArray[1] {
                    case "None": //None
                        opt.WithCompression(options.None)
                    case "Snappy": //Snappy
                        opt.WithCompression(options.Snappy)
                    case "ZSTD": //ZSTD
                        opt.WithCompression(options.ZSTD)
                    default:
                        fmt.Printf("(Ldb-Tool) Invalid Options: %s\n", cmdSplits[i])
                        return nil
            }
            case "VerifyValueChecksum": //VerifyValueChecksum
                verifyValueChecksum, _ := strconv.ParseBool(optionArray[1])
                opt.WithVerifyValueChecksum(verifyValueChecksum)
            case "ChecksumVerificationMode": //ChecksumVerificationMode
                //opt.WithChecksumVerificationMode(optionArray[1])
                switch optionArray[1] {
                    case "NoVerification": //NoVerification
                        opt.WithChecksumVerificationMode(options.NoVerification)
                    case "OnTableRead": //OnTableRead
                        opt.WithChecksumVerificationMode(options.OnTableRead)
                    case "OnBlockRead": //OnBlockRead
                        opt.WithChecksumVerificationMode(options.OnBlockRead)
                    case "OnTableAndBlockRead": //OnTableAndBlockRead
                        opt.WithChecksumVerificationMode(options.OnTableAndBlockRead)
                    default:
                        fmt.Printf("(Ldb-Tool) Invalid Options: %s\n", cmdSplits[i])
                        return nil
            }
            case "AllowStopTheWorld": //AllowStopTheWorld
                allowStopTheWorld, _ := strconv.ParseBool(optionArray[1])
                opt.WithAllowStopTheWorld(allowStopTheWorld)
            case "BlockCacheSize": //BlockCacheSize
                blockCacheSize, _ := strconv.ParseInt(optionArray[1], 10, 64)
                opt.WithBlockCacheSize(blockCacheSize)
            case "InMemory": //InMemory
                inMemory, _ := strconv.ParseBool(optionArray[1])
                opt.WithInMemory(inMemory)
            case "ZSTDCompressionLevel": //ZSTDCompressionLevel
                zSTDCompressionLevel, _ := strconv.Atoi(optionArray[1])
                opt.WithZSTDCompressionLevel(zSTDCompressionLevel)
            case "BypassLockGuard": //BypassLockGuard
                bypassLockGuard, _ := strconv.ParseBool(optionArray[1])
                opt.WithBypassLockGuard(bypassLockGuard)
            case "IndexCacheSize": //IndexCacheSize
                indexCacheSize, _ := strconv.ParseInt(optionArray[1], 10, 64)
                opt.WithIndexCacheSize(indexCacheSize)
            case "DetectConflicts": //DetectConflicts
                detectConflicts, _ := strconv.ParseBool(optionArray[1])
                opt.WithDetectConflicts(detectConflicts)
            case "NamespaceOffset": //NamespaceOffset
                namespaceOffset, _ := strconv.Atoi(optionArray[1])
                opt.WithNamespaceOffset(namespaceOffset)
            case "TableSizeMultiplier": //TableSizeMultiplier
                tableSizeMultiplier, _ := strconv.Atoi(optionArray[1])
                opt.TableSizeMultiplier = tableSizeMultiplier
            case "LmaxCompaction": //LmaxCompaction
                lmaxCompaction, _ := strconv.ParseBool(optionArray[1])
                opt.LmaxCompaction = lmaxCompaction
            default:
                fmt.Printf("(Ldb-Tool) Invalid Options: %s\n", cmdSplits[i])
                return nil
            }
        } 
    }
 
    db, err := badger.Open(opt)
    if err != nil {
        //log.Fatal(err)
        fmt.Println(err)
        return nil
    }else{
        fmt.Println("OK")
        return db
    }  
}

func Put(cmdSplits []string, db *badger.DB) {
    key := []byte(cmdSplits[1])
    value := []byte(cmdSplits[2])
    err := db.Update(func(txn *badger.Txn) error {
        entry := badger.NewEntry(key, value)
        if err := txn.SetEntry(entry); err != nil {
            return err
        }
        fmt.Println("OK")
        return nil
    })

    if err != nil {
        //log.Fatal(err)
        fmt.Println(err)
    }
}

func Get(cmdSplits []string, db *badger.DB) {
    key := []byte(cmdSplits[1])
    err := db.View(func(txn *badger.Txn) error {
        item, err := txn.Get(key)
        if err != nil {
            return err
        }
        if err := item.Value(func(val []byte) error {
            fmt.Printf("OK: %q \n", val)
            return nil
        }); err != nil {
            return err
        }
        return nil
    })

    if err != nil {
        //log.Fatal(err)
        fmt.Println(err)
    }
}

func Delete(cmdSplits []string, db *badger.DB) {
    key := []byte(cmdSplits[1])
    txn := db.NewTransaction(true)
    err := txn.Delete(key)
    if err != nil {
        //log.Fatal(err)
        fmt.Println(err)
    }
    // Commit the transaction and check for error.
    if err = txn.Commit(); err != nil {
        fmt.Println(err)
    }
    fmt.Println("OK")
}

func Write(cmdSplits []string, db *badger.DB) {
    if len(cmdSplits) == 1 {
        fmt.Printf("(Ldb-Tool) Invalid Command: %s\n", cmdSplits)
        return
    }

    wb := db.NewWriteBatch()
    defer wb.Cancel()

    for i := 1; i < len(cmdSplits); i++ {
        //entry := badger.NewEntry(key, value)
        keyValueArray := strings.Split(cmdSplits[i], ":")
        if keyValueArray[0] == "put"{
            key := []byte(keyValueArray[1])
            value := []byte(keyValueArray[2])
            wb.Set(key, value)

        }else if keyValueArray[0] == "delete"{
            key := []byte(keyValueArray[1])
            wb.Delete(key)

        }else{
            //fmt.Printf("(Ldb-Tool) Invalid Command: %s\n", cmdSplits)
            //return
        }
    }

    err := wb.Flush()
    if err != nil {
        //log.Fatal(err)
        fmt.Println(err)
    }else{
        fmt.Println("OK")
    }
}

func Iterator(cmdSplits []string, db *badger.DB) {

    iterOpt := badger.DefaultIteratorOptions
    iterOpt.PrefetchValues = false
    txn := db.NewTransaction(false)
    idxIt := txn.NewIterator(iterOpt)
    defer idxIt.Close()
    
    var keyPrefix []byte
    var keyVal string
    cmdTokenIndex := 0
    for i := 0; i < len(cmdSplits); i++ {

        if !strings.HasPrefix(cmdSplits[i], "--") {
            //fmt.Printf("Ldb Command name: %s \n", cmdSplits[i])
            if cmdTokenIndex == 1 {
                //fmt.Printf("Ldb Command key: %s \n", cmdSplits[i])
                keyValueArray := strings.Split(cmdSplits[i], ":")
                if keyValueArray[0] == "seek" && len(keyValueArray) == 2{
                    keyPrefix = []byte(keyValueArray[1])
        
                }else if keyValueArray[0] == "seektofirst"{
                    for idxIt.Rewind(); idxIt.Valid(); idxIt.Next() {
                        key := idxIt.Item().Key()
                        //fmt.Printf("%q:", key)
                        if err := idxIt.Item().Value(func(val []byte) error {
                            //fmt.Printf("%q ", val)
                            keyVal = fmt.Sprintf("%s %s %s", keyVal, key, val)
                            return nil
                        }); err != nil {
                            //log.Fatal(err)
                            fmt.Println(err)
                        }
                    }
                    fmt.Printf("OK:%s\n", keyVal)
                    return
                }else{
                    fmt.Printf("(Ldb-Tool) Invalid Command: %v\n", cmdSplits)
                    return
                }
            }
            if cmdTokenIndex == 2 {
                //fmt.Printf("Ldb Command key: %s \n", cmdSplits[i])
                if cmdSplits[i] == "validforprefix" {
                    for idxIt.Seek(keyPrefix); idxIt.ValidForPrefix(keyPrefix); idxIt.Next() {
                        key := idxIt.Item().Key()
                        //fmt.Printf("%q:", key)
                        if err := idxIt.Item().Value(func(val []byte) error {
                            //fmt.Printf("%q ", val)
                            keyVal = fmt.Sprintf("%s %s:%s", keyVal, key, val)
                            return nil
                        }); err != nil {
                            //log.Fatal(err)
                            fmt.Println(err)
                        }
                    }
                }else if cmdSplits[i] == "valid" {
                    for idxIt.Seek(keyPrefix); idxIt.Valid(); idxIt.Next() {
                        key := idxIt.Item().Key()
                        //fmt.Printf("%q:", key)
                        if err := idxIt.Item().Value(func(val []byte) error {
                            //fmt.Printf("%q ", val)
                            keyVal = fmt.Sprintf("%s %s:%s", keyVal, key, val)
                            return nil
                        }); err != nil {
                            //log.Fatal(err)
                            fmt.Println(err)
                        }
                    }
                }else{
                    fmt.Printf("(Ldb-Tool) Invalid Command: %s\n", cmdSplits)
                    return
                }
                fmt.Printf("OK:%s\n", keyVal)
                return
            } 
            cmdTokenIndex++
        }
    }
}

func Close(db *badger.DB) {
    //fmt.Println("Invoke close")
    err := db.Close()
    if err != nil {
        //log.Fatal(err)
        fmt.Println(err)
    }else{
        fmt.Println("OK")
    }
}

func main() {
    
    var db *badger.DB
    db = nil
    inputReader := bufio.NewReader(os.Stdin)
    //fmt.Printf("Please enter Ldb Command:")
    for {
        input, err := inputReader.ReadString('\n')
        if err != nil {
            fmt.Println("There were errors reading, exiting program.")
            return
        }
        //fmt.Printf("Ldb Command is %s", input)

        cmdSplits:= strings.Fields(input)
        //fmt.Println(cmdSplits)
        err = nil
        switch cmdSplits[0] {
        case "opendb":
            //fmt.Println("Invoke opendb")
            db = Opendb(cmdSplits)
        case "put":
            //fmt.Println("Invoke put")
            Put(cmdSplits, db)
        case "get":
            //fmt.Println("Invoke get")
            Get(cmdSplits, db)
        case "close":
            //fmt.Println("Invoke close")
            Close(db)
        case "merge":
            fmt.Println("Invoke merge")
        case "write":
            //fmt.Println("Invoke write")
            Write(cmdSplits, db)
        case "iterator":
            //fmt.Println("Invoke iterator")
            Iterator(cmdSplits, db)            
        case "delete":
            //fmt.Println("Invoke delete")
            Delete(cmdSplits, db)
        case "exit":
            //fmt.Println("Invoke exit")
            return
        default:
            fmt.Printf("(Ldb-Tool) Invalid Command: %s \n", input)
        }
    }
}
