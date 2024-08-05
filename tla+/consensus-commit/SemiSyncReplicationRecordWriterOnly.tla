------------------------ MODULE SemiSyncReplicationRecordWriterOnly ------------------------
EXTENDS Integers, Sequences, TLC
CONSTANT Null, Undefined, RecordWriterProcesses, MaxRetryOfEnqueue, OperationTypeIndex
ASSUME RecordWriterProcesses /= {}
ASSUME MaxRetryOfEnqueue >= 0
ASSUME OperationTypeIndex \in {1, 2}

(* For instance, if the parameters are as follows,
   - xs is: { [k1 |-> "v1_a", k2 |-> "v2_a"], [k1 |-> "v1_b", k2 |-> "v2_b"] }
   - key: "k2"
   - default_value: Null
   the following function is returned
   - [ v2_a |-> Null, v2_b |-> Null ]
*)
CreateFunctionFromSetWithKey(xs, key, default_value) ==
    LET keys == {x[key] : x \in xs}
    IN [k \in keys |-> default_value]

LastItemOfSequence(xs) ==
    IF xs = <<>> THEN Undefined ELSE xs[Len(xs)]

(* For instance, if the parameters are as follows,
   - ops:
     - [type |-> "insert", tx_id |-> "t1", prev_tx_id |-> Null]
     - [type |-> "update", tx_id |-> "t2", prev_tx_id |-> "t2"]
     - [type |-> "delete", tx_id |-> "t3", prev_tx_id |-> "t2"]
     - [type |-> "insert", tx_id |-> "t9", prev_tx_id |-> Null]
   - candidate_separate_ops:
     - ins_ops |-> {}
     - non_ins_ops |-> [t1 |-> Null, t2 |-> Null]
   - inserted_tx_ids: ["t9"]
   the following function is returned
   - ins_ops |-> [type |-> "insert", tx_id |-> "t1", prev_tx_id |-> Null]
   - non_ins_ops |-> [
       t1 |-> [type |-> "update", tx_id |-> "t2", prev_tx_id |-> "t1"],
       t2 |-> [type |-> "delete", tx_id |-> "t3", prev_tx_id |-> "t2"]
     ]
   
   Insert operations are de-duplicated.
*)
BuildOpsForInsertAndOpsForNonInsert(ops, candidate_separate_ops, fetched_xs) ==
    LET
        RECURSIVE BuildOpsForInsertAndOpsForNonInsert_(_, _, _)
        BuildOpsForInsertAndOpsForNonInsert_(ops_, ins_ops, non_ins_ops) ==
        IF ops_ = {} THEN
            [ins_ops |-> ins_ops, non_ins_ops |-> non_ins_ops]
        ELSE
            LET
                op == CHOOSE op \in ops_: TRUE
                new_ops == ops_ \ {op}
            IN
            IF op["type"] = "insert" THEN
                IF op["tx_id"] \notin fetched_xs THEN
                    BuildOpsForInsertAndOpsForNonInsert_(new_ops, ins_ops \union {op}, non_ins_ops)
                ELSE
                    BuildOpsForInsertAndOpsForNonInsert_(new_ops, ins_ops, non_ins_ops)
            ELSE
                BuildOpsForInsertAndOpsForNonInsert_(new_ops, ins_ops, [non_ins_ops EXCEPT ![op["prev_tx_id"]] = op])
    IN BuildOpsForInsertAndOpsForNonInsert_(ops, candidate_separate_ops["ins_ops"], candidate_separate_ops["non_ins_ops"])

ComputeSecondaryDbRecord(ops_to_be_moved, applied_tx_ids_on_secondary) ==
    LET
        RECURSIVE ComputeSecondaryDbRecord_(_, _)
            ComputeSecondaryDbRecord_(ops_to_be_moved_, applied_tx_ids_on_secondary_) ==
            IF ops_to_be_moved_ = <<>> THEN
                applied_tx_ids_on_secondary_
            ELSE
                LET
                    op == Head(ops_to_be_moved_)
                    remaining_ops_to_be_moved == Tail(ops_to_be_moved_)
                IN
                    CASE op["type"] = "insert" -> ComputeSecondaryDbRecord_(remaining_ops_to_be_moved, <<op["tx_id"]>>)
                        [] op["type"] = "update" -> ComputeSecondaryDbRecord_(remaining_ops_to_be_moved, Append(applied_tx_ids_on_secondary_, op["tx_id"]))
                        [] TRUE                  -> ComputeSecondaryDbRecord_(remaining_ops_to_be_moved, <<>>)
    IN ComputeSecondaryDbRecord_(ops_to_be_moved, applied_tx_ids_on_secondary)

ExtractInsertOpTxIds(ops_to_be_moved) ==
  LET
    RECURSIVE ExtractInsertOpTxIds_(_, _)
    ExtractInsertOpTxIds_(result, remaining) ==
        IF remaining = <<>> THEN
            result
        ELSE
            LET
                hd == Head(remaining)
                tl == Tail(remaining)
            IN
            IF hd["type"] = "insert" THEN
                ExtractInsertOpTxIds_(result \union {hd["tx_id"]}, tl)
            ELSE
                ExtractInsertOpTxIds_(result, tl)
    IN ExtractInsertOpTxIds_({}, ops_to_be_moved)

HasSamePrefix(expected, xs) ==
    LET
        RECURSIVE HasSamePrefix_(_, _)
        HasSamePrefix_(expected_, xs_) ==
        IF xs_ = <<>> THEN
            TRUE
        ELSE
            LET
                hd_of_expected == Head(expected_)
                tl_of_expected == Tail(expected_)
                hd_of_xs == Head(xs_)
                tl_of_xs == Tail(xs_)
            IN
            IF hd_of_expected = hd_of_xs THEN
                HasSamePrefix_(tl_of_expected, tl_of_xs)
            ELSE
                FALSE
    IN HasSamePrefix_(expected, xs)

(* --algorithm SemiSyncReplicationRecordWriterOnly
variable
    stored_ops = <<
        \* Type 1
        {
            [prev_tx_id |-> Null, tx_id |-> "t1", type |-> "insert"],
            [prev_tx_id |-> "t1", tx_id |-> "t2", type |-> "update"],
            [prev_tx_id |-> "t2", tx_id |-> "t3", type |-> "delete"],
            [prev_tx_id |-> Null, tx_id |-> "t9", type |-> "insert"]
        },
        \* Type 2
        {
            [prev_tx_id |-> Null, tx_id |-> "t1", type |-> "insert"],
            [prev_tx_id |-> "t1", tx_id |-> "t2", type |-> "delete"],
            [prev_tx_id |-> Null, tx_id |-> "t3", type |-> "insert"],
            [prev_tx_id |-> "t3", tx_id |-> "t4", type |-> "delete"],
            [prev_tx_id |-> Null, tx_id |-> "t9", type |-> "insert"]
        }
    >>[OperationTypeIndex],

    expected_applied_tx_id_on_secondary_db = <<
        \* Type 1
        {
            <<"t1", "t2", "t3", "t9">>,
            <<"t9">>
        },
        \* Type 2
        {
            <<"t1", "t2", "t3", "t4", "t9">>,
            <<"t1", "t2", "t9">>,
            <<"t3", "t4", "t1", "t2", "t9">>,
            <<"t3", "t4", "t9">>,
            <<"t9">>
        }
    >>[OperationTypeIndex],

    repl_db_record = [
        tx_id |-> Null,
        deleted |-> Null,
        prep_tx_id |-> Null,
        version |-> Null,
        \* Probably this can be removed
        insert_tx_ids |-> {},
        ops |-> {}
    ],

    secondary_db_record = [
        tx_id |-> Null,
        deleted |-> Null,
        applied_tx_ids |-> <<>>
    ],

    retry_of_enqueue = 0
    ;

define
    CurrentRecordExists(record) == record["tx_id"] /= Null /\ record["deleted"] = FALSE

    ExpectedState ==
        stored_ops = {}
        /\
        (
            (~CurrentRecordExists(repl_db_record) /\ {op \in repl_db_record["ops"] : op["type"] = "insert"} = {})
            \/ (CurrentRecordExists(repl_db_record) /\ {op \in repl_db_record["ops"] : op["prev_tx_id"] = repl_db_record["tx_id"]} = {})
        )
        /\ LastItemOfSequence(secondary_db_record["applied_tx_ids"]) = "t9"

    EventuallyExpectedState == <>[]ExpectedState

    \* Invaliants
    TooLargeVersionIsUnexpected == repl_db_record["version"] = Null \/ repl_db_record["version"] < 200

    OpsInCurrentRecordAreValid ==
        \A op \in repl_db_record.ops:
            op["tx_id"] /= Null
            /\ (
                (op["type"] = "insert" /\ op["prev_tx_id"] = Null) \/
                (op["type"] = "update" /\ op["prev_tx_id"] /= Null) \/
                (op["type"] = "delete" /\ op["prev_tx_id"] /= Null)
            )
end define;

fair process RecordWriter \in RecordWriterProcesses
variables
    need_to_stop = FALSE;

    fetched_cur_record = Null;

    new_cur_record_info = Null;

    tmp_op = Null;

    ops_to_be_moved = Null;

    tmp_applied_tx_ids_on_secondary_db = Null;

    failed_to_write_to_secondary_db = Null;

    candidate_separate_ops = Null;

begin
    Repeat:
    while ~ExpectedState do
        (* Move operations from `transaction` table to `record` table in Replication DB.
           Ideally, a dedicated worker process to handle the task should exist.
           However, RecordWriter processes handle it to reduce the state space.
        *)
        if stored_ops /= {} then
            with xs \in {xs \in SUBSET stored_ops : xs /= {}} do
                if repl_db_record["version"] = Null then 
                    repl_db_record := [repl_db_record EXCEPT
                        !.ops = repl_db_record["ops"] \union xs,
                        !.version = 1
                    ];
                else
                    repl_db_record := [repl_db_record EXCEPT
                        !.ops = repl_db_record["ops"] \union xs,
                        !.version = repl_db_record["version"] + 1
                    ];
                end if; 
                if retry_of_enqueue <= MaxRetryOfEnqueue then
                    retry_of_enqueue := retry_of_enqueue + 1;
                    either
                        stored_ops := stored_ops \ xs;
                    or
                        skip;
                    end either;
                else
                    stored_ops := stored_ops \ xs;
                end if;
            end with;
        end if;

        \* Fetch the current record from `record` table in Repl DB and initialize the local variables.
        need_to_stop := FALSE;

        fetched_cur_record := repl_db_record;

        new_cur_record_info := [
            tx_id |-> fetched_cur_record["tx_id"],
            deleted |-> fetched_cur_record["deleted"]
        ];

        failed_to_write_to_secondary_db := FALSE;

        \* This contains two keys "ins_ops" and "non_ins_ops" which have `insert` operations and `update`/`delete` operations, respectively.
        candidate_separate_ops := BuildOpsForInsertAndOpsForNonInsert(
                    fetched_cur_record["ops"],
                    [
                        ins_ops |-> {},
                        non_ins_ops |-> CreateFunctionFromSetWithKey(
                            {x \in fetched_cur_record["ops"] : x["type"] /= "insert"},
                            "prev_tx_id",
                            Null
                        )
                    ],
                    fetched_cur_record["insert_tx_ids"]
                );
        assert \A k \in DOMAIN candidate_separate_ops["non_ins_ops"] : candidate_separate_ops["non_ins_ops"][k] # Null;
        
        ops_to_be_moved := <<>>;

        (* Build the next Replication DB record `new_cur_record_info`
        and the operations to be moved to the secondary DB record `ops_to_be_moved`
        based on `candidate_separate_ops`.
        *)
        BuildNextOperationByMergingOperationChain:
        while ~need_to_stop do
            if ~CurrentRecordExists(new_cur_record_info) /\ candidate_separate_ops["ins_ops"] /= {} then
                (* No target record exists in `record` table in Replication DB
                   because this is the first operation or the record was deleted.
                   Therefore, only insert operations are allowed.
                *)

                \* When `prep_tx_id` is set, an insert op that has the same tx_id must exist.
                assert fetched_cur_record["prep_tx_id"] = Null \/ \E x \in candidate_separate_ops["ins_ops"] : x["tx_id"] = fetched_cur_record["prep_tx_id"];

                if fetched_cur_record["prep_tx_id"] /= Null then
                    (* A prepared transaction ID exists. It means another thread has tried or is tring to replicate the operations.
                        The ongoing replication needs to be continued.
                    *)

                    with x \in {x \in candidate_separate_ops["ins_ops"] : x["tx_id"] = fetched_cur_record["prep_tx_id"]} do
                        tmp_op := x;
                    end with;
                else
                    \* No prepared transaction ID exists. It means no thread has tried or is tring to replicate operations.
                    with x \in candidate_separate_ops["ins_ops"] do
                        tmp_op := x;
                    end with;
                end if;
                assert tmp_op["type"] = "insert";
                candidate_separate_ops["ins_ops"] := candidate_separate_ops["ins_ops"] \ {tmp_op};
                new_cur_record_info := [new_cur_record_info EXCEPT
                    !.tx_id = tmp_op["tx_id"],
                    !.deleted = FALSE
                ];
                ops_to_be_moved := Append(ops_to_be_moved, tmp_op);
                (* Don't continue when picking an insert operation considering `prep_tx_id` and remaning insert stored_ops *)
                need_to_stop := TRUE;
            elsif CurrentRecordExists(new_cur_record_info)
                /\ new_cur_record_info["tx_id"] \in (DOMAIN candidate_separate_ops["non_ins_ops"]) then
                (* The target record exists in `record` table in Replication DB.
                Therefore, only update or delete operations which follows the current record are allowed.
                *)
                tmp_op := candidate_separate_ops["non_ins_ops"][new_cur_record_info["tx_id"]];
                candidate_separate_ops["non_ins_ops"] := [
                    tx_id \in DOMAIN candidate_separate_ops["non_ins_ops"] \ {new_cur_record_info["tx_id"]} |->
                        candidate_separate_ops["non_ins_ops"][tx_id]
                ];
                if tmp_op["type"] = "update" then
                    new_cur_record_info := [new_cur_record_info EXCEPT
                        !.tx_id = tmp_op["tx_id"],
                        !.deleted = FALSE
                    ];
                elsif tmp_op["type"] = "delete" then
                    new_cur_record_info := [new_cur_record_info EXCEPT
                        !.tx_id = tmp_op["tx_id"],
                        !.deleted = TRUE
                    ];
                else
                    assert FALSE;
                end if;
                ops_to_be_moved := Append(ops_to_be_moved, tmp_op);

                \* The current merged operation is the same as the prepared state. Let's stop the iteration.
                if fetched_cur_record["prep_tx_id"] /= Null /\ tmp_op["tx_id"] = fetched_cur_record["prep_tx_id"] then
                    need_to_stop := TRUE;
                else
                    need_to_stop := FALSE;
                end if;
            else
                \* Next operation isn't found. Let's stop this iteration.
                tmp_op := Null;
                need_to_stop := TRUE;
            end if;

            if fetched_cur_record["prep_tx_id"] /= Null /\ need_to_stop /\ ops_to_be_moved /= <<>> then
                (* If stopping the iteration with a prepared transaction ID is set,
                   the replicated operation's transaction ID must be same as the prepared transaction ID.
                *)
                assert tmp_op["tx_id"] = fetched_cur_record["prep_tx_id"];
            end if;
        end while;

        if ops_to_be_moved /= <<>> then
            SetPrepTxIdOfReplDbRecord:
            \* There are operations to be replicated to the secondary DB.
            if repl_db_record["version"] = fetched_cur_record["version"] /\ repl_db_record["prep_tx_id"] = fetched_cur_record["prep_tx_id"] then
                (* Continue the replication only if no other thread has replicated the same record in conflict
                   by checking `record` table in Replication DB.
                *)

                \* Set `record.prep_tx_id` to the calculated `tx_id`.
                repl_db_record["prep_tx_id"] := new_cur_record_info["tx_id"];

                WriteOpsToSecondaryDbRecord:
                tmp_applied_tx_ids_on_secondary_db := ComputeSecondaryDbRecord(ops_to_be_moved, secondary_db_record["applied_tx_ids"]);

                (* Note: Very delayed INSERT could try to write to the secondary DB even after a subsequent DELETE operation is executed.
                   Deleted record with `deleted:true` remains on the secondary DB considering this case.
                   The point is the delayed thread couldn't notice newer delete operations are applied on the secondary DB,
                   but it can notice any new operation is applied to the secondary DB even if the new opeartion is delete operation.
                
                ,-.             ,-.                                                                            
                `-'             `-'                                                                            
                /|\             /|\                                                                            
                 |               |                ,-------.          ,------------.                            
                / \             / \               |Repl_DB|          |Secondary_DB|                            
               Txn1            Txn2               `---+---'          `-----+------'                            
                |               |                     |  ,-------------------------!.                          
                |               |                     |  |ver: 1                   |_\                         
                |               |                     |  |tx_id: Null                |                         
                |               |                     |  |prep_tx_id: Null           |                         
                |               |                     |  |ops: [                     |                         
                |               |                     |  |  Insert(Null -> tx_id:1)  |                         
                |               |                     |  |]                          |                         
                |               |                     |  `---------------------------'                         
                |          FetchCurrentRecord         |                    |                                   
                | ------------------------------------>                    |                                   
                |               |                     |                    |                                   
                |  ,-------------------------!.       |                    |                                   
                |  |ver: 1                   |_\      |                    |                                   
                |  |tx_id: Null                |      |                    |                                   
                |  |<b>prep_tx_id: tx_id:1     |      |                    |                                   
                |  |ops: [                     |      |                    |                                   
                |  |  Insert(Null -> tx_id:1)  |      |                    |                                   
                |  |]                          |      |                    |                                   
                |  `---------------------------'      |                    |                                   
                |         PrepareTxId(tx_id:1)        |                    |                                   
                | ------------------------------------>                    |                                   
                |               |                     |                    |                                   
                |               |                     |  ,-------------------------!.                          
                |               |                     |  |ver: 1                   |_\                         
                |               |                     |  |tx_id: Null                |                         
                |               |                     |  |<b>prep_tx_id: tx_id:1     |                         
                |               |                     |  |ops: [                     |                         
                |               |                     |  |  Insert(Null -> tx_id:1)  |                         
                |               |                     |  |]                          |                         
                |               |                     |  `---------------------------'                         
                |  ,------!.    |                     |                    |                                   
                |  |Stuck!|_\   |                     |                    |                                   
                |  `--------'   |                     |                    |                                   
                |               |  FetchCurrentRecord |                    |                                   
                |               | -------------------->                    |                                   
                |               |                     |                    |                                   
                |               |  ,-------------------------!.            |                                   
                |               |  |ver: 1                   |_\           |                                   
                |               |  |tx_id: Null                |           |                                   
                |               |  |prep_tx_id: tx_id:1        |           |                                   
                |               |  |ops: [                     |           |                                   
                |               |  |  Insert(Null -> tx_id:1)  |           |                                   
                |               |  |]                          |           |                                   
                |               |  `---------------------------'           |                                   
                |               | PrepareTxId(tx_id:1)|                    |                                   
                |               | -------------------->                    |                                   
                |               |                     |                    |                                   
                |               |                     |  ,-------------------------!.                          
                |               |                     |  |ver: 1                   |_\                         
                |               |                     |  |tx_id: Null                |                         
                |               |                     |  |prep_tx_id: tx_id:1        |                         
                |               |                     |  |ops: [                     |                         
                |               |                     |  |  Insert(Null -> tx_id:1)  |                         
                |               |                     |  |]                          |                         
                |               |                     |  `---------------------------'                         
                |               |     WriteOperations |                    |                                   
                |               |     (cur_tx_id:Null,|                    |                                   
                |               |      new_tx: Insert(Null -> tx_id:1))    |                                   
                |               | ----------------------------------------->                                   
                |               |                     |                    |                                   
                |               |                     |                    |  ,-------------------------!.     
                |               |                     |                    |  |tx_id: tx_id:1           |_\    
                |               |                     |                    |  |deleted: false             |    
                |               |                     |                    |  |applied_tx_ids:[           |    
                |               |                     |                    |  |  Insert(Null -> tx_id:1)  |    
                |               |                     |                    |  |]                          |    
                |               |                     |                    |  `---------------------------'    
                |               | UpdateCurrentRecord |                    |                                   
                |               | (ver:2,             |                    |                                   
                |               |  tx_id:1,           |                    |                                   
                |               |  prep_tx_id:Null,   |                    |                                   
                |               |  ops: [])           |                    |                                   
                |               | -------------------->                    |                                   
                |               |                     |                    |                                   
                |               |                     |  ,-------------------!.                                
                |               |                     |  |<b>ver: 2          |_\                               
                |               |                     |  |<b>tx_id: tx_id:1    |                               
                |               |                     |  |<b>prep_tx_id: Null  |                               
                |               |                     |  |<b>ops: []           |                               
                |               |                     |  `---------------------'                               
                |               |                     |  ,-------------------------------!.                    
                |               |                     |  |New operation is added!        |_\                   
                |               |                     |  |                                 |                   
                |               |                     |  |<b>ver: 3                        |                   
                |               |                     |  |tx_id: tx_id:1                   |                   
                |               |                     |  |prep_tx_id: Null                 |                   
                |               |                     |  |<b>ops: [                        |                   
                |               |                     |  |<b>  Delete(tx_id:1 -> tx_id:2)  |                   
                |               |                     |  |<b>]                             |                   
                |               |                     |  `---------------------------------'                   
                |               |  FetchCurrentRecord |                    |                                   
                |               | -------------------->                    |                                   
                |               |                     |                    |                                   
                |               |  ,----------------------------!.         |                                   
                |               |  |ver: 3                      |_\        |                                   
                |               |  |tx_id: tx_id:1                |        |                                   
                |               |  |<b>prep_tx_id: tx_id:2        |        |                                   
                |               |  |ops: [                        |        |                                   
                |               |  |  Delete(tx_id:1 -> tx_id:2)  |        |                                   
                |               |  |]                             |        |                                   
                |               |  `------------------------------'        |                                   
                |               | PrepareTxId(tx_id:2)|                    |                                   
                |               | -------------------->                    |                                   
                |               |                     |                    |                                   
                |               |                     |  ,----------------------------!.                       
                |               |                     |  |ver: 3                      |_\                      
                |               |                     |  |tx_id: tx_id:1                |                      
                |               |                     |  |<b>prep_tx_id: tx_id:2        |                      
                |               |                     |  |ops: [                        |                      
                |               |                     |  |  Delete(tx_id:1 -> tx_id:2)  |                      
                |               |                     |  |]                             |                      
                |               |                     |  `------------------------------'                      
                |               |   WriteOperations   |                    |                                   
                |               |   (cur_tx_id: tx_id:1,                   |                                   
                |               |    new_tx: Delete(tx_id:1 -> tx_id:2))   |                                   
                |               | ----------------------------------------->                                   
                |               |                     |                    |                                   
                |               |                     |                    |  ,----------------------------!.  
                |               |                     |                    |  |tx_id: tx_id:2              |_\ 
                |               |                     |                    |  |<b>deleted: true              | 
                |               |                     |                    |  |applied_tx_ids:[              | 
                |               |                     |                    |  |  Insert(Null -> tx_id:1),    | 
                |               |                     |                    |  |  Delete(tx_id:1 -> tx_id:2)  | 
                |               |                     |                    |  |]                             | 
                |               |                     |                    |  `------------------------------' 
                |               | UpdateCurrentRecord |                    |                                   
                |               | (ver:3,             |                    |                                   
                |               |  tx_id:2,           |                    |                                   
                |               |  deleted:true,      |                    |                                   
                |               |  prep_tx_id:Null,   |                    |                                   
                |               |  ops: [])           |                    |                                   
                |               | -------------------->                    |                                   
                |               |                     |                    |                                   
                |               |                     |  ,--------------------!.                               
                |               |                     |  |ver: 3              |_\                              
                |               |                     |  |tx_id: tx_id:2        |                              
                |               |                     |  |<b>deleted: true</b>  |                              
                |               |                     |  |prep_tx_id: Null      |                              
                |               |                     |  |ops: []               |                              
                |               |                     |  `----------------------'                              
                |  ,-----------!.                     |                    |                                   
                |  |Resuming...|_\                    |                    |                                   
                |  `-------------'                    |                    |                                   
                |             WriteOperations         |                    |                                   
                |             (cur_tx_id: Null,       |                    |                                   
                |              new_tx: Insert(Null -> tx_id:1))            |                                   
                | --------------------------------------------------------->                                   
                |               |                     |                    |                                   
                |               |                     |                    |  ,-----------------------------!. 
                |               |                     |                    |  |Secondary_DB can reject the  |_\
                |               |                     |                    |  |delayed request as the         |
                |               |                     |                    |  |`cur_tx_id` doesn't match      |
                |               |                     |                    |  |with the stored one thanks to  |
                |               |                     |                    |  |the remaining record with      |
                |               |                     |                    |  |<b>deleted:true</b>.           |
               Txn1            Txn2               ,---+---.          ,-----+--`-------------------------------'
                ,-.             ,-.               |Repl_DB|          |Secondary_DB|                            
                `-'             `-'               `-------'          `------------'                            
                /|\             /|\                                                                            
                 |               |                                                                             
                / \             / \                                                                            

                *)

                \* Write the operation to the secondary DB record using CAS.
                if secondary_db_record["tx_id"] /= fetched_cur_record["tx_id"] then
                    RecheckSecondaryDbRecordIsUpdated:
                    if secondary_db_record["tx_id"] /= fetched_cur_record["prep_tx_id"] then
                        \* This can happen when a previous transaction failed right after write to secondary DB...
                        failed_to_write_to_secondary_db := TRUE;
                    else
                        \* Leave the deleted record as "deleted" status.
                        secondary_db_record := [
                            tx_id |-> new_cur_record_info["tx_id"],
                            deleted |-> new_cur_record_info["deleted"],
                            applied_tx_ids |-> tmp_applied_tx_ids_on_secondary_db
                        ];
                    end if;
                else
                    \* Leave the deleted record as "deleted" status.
                    secondary_db_record := [
                        tx_id |-> new_cur_record_info["tx_id"],
                        deleted |-> new_cur_record_info["deleted"],
                        applied_tx_ids |-> tmp_applied_tx_ids_on_secondary_db
                    ];
                end if;

                UpdateCurrentReplDbRecord:
                (* Update `record` table in Replcation DB if the operation is written to the secondary DB
                and there is no other thread that has already updated the record.

                If the record in `record` table in Replication DB is updated,
                it means other thread took over the same operations and the expected operation must be written to the secondary DB record.
                *)
                if ~failed_to_write_to_secondary_db /\ fetched_cur_record["version"] = repl_db_record["version"] then
                    repl_db_record := [
                        tx_id |-> new_cur_record_info["tx_id"],
                        deleted |-> new_cur_record_info["deleted"],
                        prep_tx_id |-> Null,
                        version |-> fetched_cur_record["version"] + 1,
                        \* `insert_tx_ids` must contain new insert operation tx_id
                        insert_tx_ids |-> fetched_cur_record["insert_tx_ids"] \union ExtractInsertOpTxIds(ops_to_be_moved),
                        \* `ops` must contain the rest of operations
                        ops |-> (candidate_separate_ops["ins_ops"]
                                \union {candidate_separate_ops["non_ins_ops"][x] : x \in DOMAIN candidate_separate_ops["non_ins_ops"]}) \ {Null}
                    ]
                end if;
            end if;
        end if;
    end while;
end process;

end algorithm *)
\* BEGIN TRANSLATION (chksum(pcal) = "df4b28da" /\ chksum(tla) = "73f7bd58")
VARIABLES stored_ops, expected_applied_tx_id_on_secondary_db, repl_db_record, 
          secondary_db_record, retry_of_enqueue, pc

(* define statement *)
CurrentRecordExists(record) == record["tx_id"] /= Null /\ record["deleted"] = FALSE

ExpectedState ==
    stored_ops = {}
    /\
    (
        (~CurrentRecordExists(repl_db_record) /\ {op \in repl_db_record["ops"] : op["type"] = "insert"} = {})
        \/ (CurrentRecordExists(repl_db_record) /\ {op \in repl_db_record["ops"] : op["prev_tx_id"] = repl_db_record["tx_id"]} = {})
    )
    /\ LastItemOfSequence(secondary_db_record["applied_tx_ids"]) = "t9"

EventuallyExpectedState == <>[]ExpectedState


TooLargeVersionIsUnexpected == repl_db_record["version"] = Null \/ repl_db_record["version"] < 200

OpsInCurrentRecordAreValid ==
    \A op \in repl_db_record.ops:
        op["tx_id"] /= Null
        /\ (
            (op["type"] = "insert" /\ op["prev_tx_id"] = Null) \/
            (op["type"] = "update" /\ op["prev_tx_id"] /= Null) \/
            (op["type"] = "delete" /\ op["prev_tx_id"] /= Null)
        )

VARIABLES need_to_stop, fetched_cur_record, new_cur_record_info, tmp_op, 
          ops_to_be_moved, tmp_applied_tx_ids_on_secondary_db, 
          failed_to_write_to_secondary_db, candidate_separate_ops

vars == << stored_ops, expected_applied_tx_id_on_secondary_db, repl_db_record, 
           secondary_db_record, retry_of_enqueue, pc, need_to_stop, 
           fetched_cur_record, new_cur_record_info, tmp_op, ops_to_be_moved, 
           tmp_applied_tx_ids_on_secondary_db, 
           failed_to_write_to_secondary_db, candidate_separate_ops >>

ProcSet == (RecordWriterProcesses)

Init == (* Global variables *)
        /\ stored_ops =              <<
                        
                            {
                                [prev_tx_id |-> Null, tx_id |-> "t1", type |-> "insert"],
                                [prev_tx_id |-> "t1", tx_id |-> "t2", type |-> "update"],
                                [prev_tx_id |-> "t2", tx_id |-> "t3", type |-> "delete"],
                                [prev_tx_id |-> Null, tx_id |-> "t9", type |-> "insert"]
                            },
                        
                            {
                                [prev_tx_id |-> Null, tx_id |-> "t1", type |-> "insert"],
                                [prev_tx_id |-> "t1", tx_id |-> "t2", type |-> "delete"],
                                [prev_tx_id |-> Null, tx_id |-> "t3", type |-> "insert"],
                                [prev_tx_id |-> "t3", tx_id |-> "t4", type |-> "delete"],
                                [prev_tx_id |-> Null, tx_id |-> "t9", type |-> "insert"]
                            }
                        >>[OperationTypeIndex]
        /\ expected_applied_tx_id_on_secondary_db =                                          <<
                                                    
                                                        {
                                                            <<"t1", "t2", "t3", "t9">>,
                                                            <<"t9">>
                                                        },
                                                    
                                                        {
                                                            <<"t1", "t2", "t3", "t4", "t9">>,
                                                            <<"t1", "t2", "t9">>,
                                                            <<"t3", "t4", "t1", "t2", "t9">>,
                                                            <<"t3", "t4", "t9">>,
                                                            <<"t9">>
                                                        }
                                                    >>[OperationTypeIndex]
        /\ repl_db_record =                  [
                                tx_id |-> Null,
                                deleted |-> Null,
                                prep_tx_id |-> Null,
                                version |-> Null,
                            
                                insert_tx_ids |-> {},
                                ops |-> {}
                            ]
        /\ secondary_db_record =                       [
                                     tx_id |-> Null,
                                     deleted |-> Null,
                                     applied_tx_ids |-> <<>>
                                 ]
        /\ retry_of_enqueue = 0
        (* Process RecordWriter *)
        /\ need_to_stop = [self \in RecordWriterProcesses |-> FALSE]
        /\ fetched_cur_record = [self \in RecordWriterProcesses |-> Null]
        /\ new_cur_record_info = [self \in RecordWriterProcesses |-> Null]
        /\ tmp_op = [self \in RecordWriterProcesses |-> Null]
        /\ ops_to_be_moved = [self \in RecordWriterProcesses |-> Null]
        /\ tmp_applied_tx_ids_on_secondary_db = [self \in RecordWriterProcesses |-> Null]
        /\ failed_to_write_to_secondary_db = [self \in RecordWriterProcesses |-> Null]
        /\ candidate_separate_ops = [self \in RecordWriterProcesses |-> Null]
        /\ pc = [self \in ProcSet |-> "Repeat"]

Repeat(self) == /\ pc[self] = "Repeat"
                /\ IF ~ExpectedState
                      THEN /\ IF stored_ops /= {}
                                 THEN /\ \E xs \in {xs \in SUBSET stored_ops : xs /= {}}:
                                           /\ IF repl_db_record["version"] = Null
                                                 THEN /\ repl_db_record' =                   [repl_db_record EXCEPT
                                                                               !.ops = repl_db_record["ops"] \union xs,
                                                                               !.version = 1
                                                                           ]
                                                 ELSE /\ repl_db_record' =                   [repl_db_record EXCEPT
                                                                               !.ops = repl_db_record["ops"] \union xs,
                                                                               !.version = repl_db_record["version"] + 1
                                                                           ]
                                           /\ IF retry_of_enqueue <= MaxRetryOfEnqueue
                                                 THEN /\ retry_of_enqueue' = retry_of_enqueue + 1
                                                      /\ \/ /\ stored_ops' = stored_ops \ xs
                                                         \/ /\ TRUE
                                                            /\ UNCHANGED stored_ops
                                                 ELSE /\ stored_ops' = stored_ops \ xs
                                                      /\ UNCHANGED retry_of_enqueue
                                 ELSE /\ TRUE
                                      /\ UNCHANGED << stored_ops, 
                                                      repl_db_record, 
                                                      retry_of_enqueue >>
                           /\ need_to_stop' = [need_to_stop EXCEPT ![self] = FALSE]
                           /\ fetched_cur_record' = [fetched_cur_record EXCEPT ![self] = repl_db_record']
                           /\ new_cur_record_info' = [new_cur_record_info EXCEPT ![self] =                        [
                                                                                               tx_id |-> fetched_cur_record'[self]["tx_id"],
                                                                                               deleted |-> fetched_cur_record'[self]["deleted"]
                                                                                           ]]
                           /\ failed_to_write_to_secondary_db' = [failed_to_write_to_secondary_db EXCEPT ![self] = FALSE]
                           /\ candidate_separate_ops' = [candidate_separate_ops EXCEPT ![self] =                   BuildOpsForInsertAndOpsForNonInsert(
                                                                                                     fetched_cur_record'[self]["ops"],
                                                                                                     [
                                                                                                         ins_ops |-> {},
                                                                                                         non_ins_ops |-> CreateFunctionFromSetWithKey(
                                                                                                             {x \in fetched_cur_record'[self]["ops"] : x["type"] /= "insert"},
                                                                                                             "prev_tx_id",
                                                                                                             Null
                                                                                                         )
                                                                                                     ],
                                                                                                     fetched_cur_record'[self]["insert_tx_ids"]
                                                                                                 )]
                           /\ Assert(\A k \in DOMAIN candidate_separate_ops'[self]["non_ins_ops"] : candidate_separate_ops'[self]["non_ins_ops"][k] # Null, 
                                     "Failure of assertion at line 271, column 9.")
                           /\ ops_to_be_moved' = [ops_to_be_moved EXCEPT ![self] = <<>>]
                           /\ pc' = [pc EXCEPT ![self] = "BuildNextOperationByMergingOperationChain"]
                      ELSE /\ pc' = [pc EXCEPT ![self] = "Done"]
                           /\ UNCHANGED << stored_ops, repl_db_record, 
                                           retry_of_enqueue, need_to_stop, 
                                           fetched_cur_record, 
                                           new_cur_record_info, 
                                           ops_to_be_moved, 
                                           failed_to_write_to_secondary_db, 
                                           candidate_separate_ops >>
                /\ UNCHANGED << expected_applied_tx_id_on_secondary_db, 
                                secondary_db_record, tmp_op, 
                                tmp_applied_tx_ids_on_secondary_db >>

BuildNextOperationByMergingOperationChain(self) == /\ pc[self] = "BuildNextOperationByMergingOperationChain"
                                                   /\ IF ~need_to_stop[self]
                                                         THEN /\ IF ~CurrentRecordExists(new_cur_record_info[self]) /\ candidate_separate_ops[self]["ins_ops"] /= {}
                                                                    THEN /\ Assert(fetched_cur_record[self]["prep_tx_id"] = Null \/ \E x \in candidate_separate_ops[self]["ins_ops"] : x["tx_id"] = fetched_cur_record[self]["prep_tx_id"], 
                                                                                   "Failure of assertion at line 288, column 17.")
                                                                         /\ IF fetched_cur_record[self]["prep_tx_id"] /= Null
                                                                               THEN /\ \E x \in {x \in candidate_separate_ops[self]["ins_ops"] : x["tx_id"] = fetched_cur_record[self]["prep_tx_id"]}:
                                                                                         tmp_op' = [tmp_op EXCEPT ![self] = x]
                                                                               ELSE /\ \E x \in candidate_separate_ops[self]["ins_ops"]:
                                                                                         tmp_op' = [tmp_op EXCEPT ![self] = x]
                                                                         /\ Assert(tmp_op'[self]["type"] = "insert", 
                                                                                   "Failure of assertion at line 304, column 17.")
                                                                         /\ candidate_separate_ops' = [candidate_separate_ops EXCEPT ![self]["ins_ops"] = candidate_separate_ops[self]["ins_ops"] \ {tmp_op'[self]}]
                                                                         /\ new_cur_record_info' = [new_cur_record_info EXCEPT ![self] =                        [new_cur_record_info[self] EXCEPT
                                                                                                                                             !.tx_id = tmp_op'[self]["tx_id"],
                                                                                                                                             !.deleted = FALSE
                                                                                                                                         ]]
                                                                         /\ ops_to_be_moved' = [ops_to_be_moved EXCEPT ![self] = Append(ops_to_be_moved[self], tmp_op'[self])]
                                                                         /\ need_to_stop' = [need_to_stop EXCEPT ![self] = TRUE]
                                                                    ELSE /\ IF   CurrentRecordExists(new_cur_record_info[self])
                                                                               /\ new_cur_record_info[self]["tx_id"] \in (DOMAIN candidate_separate_ops[self]["non_ins_ops"])
                                                                               THEN /\ tmp_op' = [tmp_op EXCEPT ![self] = candidate_separate_ops[self]["non_ins_ops"][new_cur_record_info[self]["tx_id"]]]
                                                                                    /\ candidate_separate_ops' = [candidate_separate_ops EXCEPT ![self]["non_ins_ops"] =                                          [
                                                                                                                                                                             tx_id \in DOMAIN candidate_separate_ops[self]["non_ins_ops"] \ {new_cur_record_info[self]["tx_id"]} |->
                                                                                                                                                                                 candidate_separate_ops[self]["non_ins_ops"][tx_id]
                                                                                                                                                                         ]]
                                                                                    /\ IF tmp_op'[self]["type"] = "update"
                                                                                          THEN /\ new_cur_record_info' = [new_cur_record_info EXCEPT ![self] =                        [new_cur_record_info[self] EXCEPT
                                                                                                                                                                   !.tx_id = tmp_op'[self]["tx_id"],
                                                                                                                                                                   !.deleted = FALSE
                                                                                                                                                               ]]
                                                                                          ELSE /\ IF tmp_op'[self]["type"] = "delete"
                                                                                                     THEN /\ new_cur_record_info' = [new_cur_record_info EXCEPT ![self] =                        [new_cur_record_info[self] EXCEPT
                                                                                                                                                                              !.tx_id = tmp_op'[self]["tx_id"],
                                                                                                                                                                              !.deleted = TRUE
                                                                                                                                                                          ]]
                                                                                                     ELSE /\ Assert(FALSE, 
                                                                                                                    "Failure of assertion at line 334, column 21.")
                                                                                                          /\ UNCHANGED new_cur_record_info
                                                                                    /\ ops_to_be_moved' = [ops_to_be_moved EXCEPT ![self] = Append(ops_to_be_moved[self], tmp_op'[self])]
                                                                                    /\ IF fetched_cur_record[self]["prep_tx_id"] /= Null /\ tmp_op'[self]["tx_id"] = fetched_cur_record[self]["prep_tx_id"]
                                                                                          THEN /\ need_to_stop' = [need_to_stop EXCEPT ![self] = TRUE]
                                                                                          ELSE /\ need_to_stop' = [need_to_stop EXCEPT ![self] = FALSE]
                                                                               ELSE /\ tmp_op' = [tmp_op EXCEPT ![self] = Null]
                                                                                    /\ need_to_stop' = [need_to_stop EXCEPT ![self] = TRUE]
                                                                                    /\ UNCHANGED << new_cur_record_info, 
                                                                                                    ops_to_be_moved, 
                                                                                                    candidate_separate_ops >>
                                                              /\ IF fetched_cur_record[self]["prep_tx_id"] /= Null /\ need_to_stop'[self] /\ ops_to_be_moved'[self] /= <<>>
                                                                    THEN /\ Assert(tmp_op'[self]["tx_id"] = fetched_cur_record[self]["prep_tx_id"], 
                                                                                   "Failure of assertion at line 354, column 17.")
                                                                    ELSE /\ TRUE
                                                              /\ pc' = [pc EXCEPT ![self] = "BuildNextOperationByMergingOperationChain"]
                                                         ELSE /\ IF ops_to_be_moved[self] /= <<>>
                                                                    THEN /\ pc' = [pc EXCEPT ![self] = "SetPrepTxIdOfReplDbRecord"]
                                                                    ELSE /\ pc' = [pc EXCEPT ![self] = "Repeat"]
                                                              /\ UNCHANGED << need_to_stop, 
                                                                              new_cur_record_info, 
                                                                              tmp_op, 
                                                                              ops_to_be_moved, 
                                                                              candidate_separate_ops >>
                                                   /\ UNCHANGED << stored_ops, 
                                                                   expected_applied_tx_id_on_secondary_db, 
                                                                   repl_db_record, 
                                                                   secondary_db_record, 
                                                                   retry_of_enqueue, 
                                                                   fetched_cur_record, 
                                                                   tmp_applied_tx_ids_on_secondary_db, 
                                                                   failed_to_write_to_secondary_db >>

SetPrepTxIdOfReplDbRecord(self) == /\ pc[self] = "SetPrepTxIdOfReplDbRecord"
                                   /\ IF repl_db_record["version"] = fetched_cur_record[self]["version"] /\ repl_db_record["prep_tx_id"] = fetched_cur_record[self]["prep_tx_id"]
                                         THEN /\ repl_db_record' = [repl_db_record EXCEPT !["prep_tx_id"] = new_cur_record_info[self]["tx_id"]]
                                              /\ pc' = [pc EXCEPT ![self] = "WriteOpsToSecondaryDbRecord"]
                                         ELSE /\ pc' = [pc EXCEPT ![self] = "Repeat"]
                                              /\ UNCHANGED repl_db_record
                                   /\ UNCHANGED << stored_ops, 
                                                   expected_applied_tx_id_on_secondary_db, 
                                                   secondary_db_record, 
                                                   retry_of_enqueue, 
                                                   need_to_stop, 
                                                   fetched_cur_record, 
                                                   new_cur_record_info, tmp_op, 
                                                   ops_to_be_moved, 
                                                   tmp_applied_tx_ids_on_secondary_db, 
                                                   failed_to_write_to_secondary_db, 
                                                   candidate_separate_ops >>

WriteOpsToSecondaryDbRecord(self) == /\ pc[self] = "WriteOpsToSecondaryDbRecord"
                                     /\ tmp_applied_tx_ids_on_secondary_db' = [tmp_applied_tx_ids_on_secondary_db EXCEPT ![self] = ComputeSecondaryDbRecord(ops_to_be_moved[self], secondary_db_record["applied_tx_ids"])]
                                     /\ IF secondary_db_record["tx_id"] /= fetched_cur_record[self]["tx_id"]
                                           THEN /\ pc' = [pc EXCEPT ![self] = "RecheckSecondaryDbRecordIsUpdated"]
                                                /\ UNCHANGED secondary_db_record
                                           ELSE /\ secondary_db_record' =                        [
                                                                              tx_id |-> new_cur_record_info[self]["tx_id"],
                                                                              deleted |-> new_cur_record_info[self]["deleted"],
                                                                              applied_tx_ids |-> tmp_applied_tx_ids_on_secondary_db'[self]
                                                                          ]
                                                /\ pc' = [pc EXCEPT ![self] = "UpdateCurrentReplDbRecord"]
                                     /\ UNCHANGED << stored_ops, 
                                                     expected_applied_tx_id_on_secondary_db, 
                                                     repl_db_record, 
                                                     retry_of_enqueue, 
                                                     need_to_stop, 
                                                     fetched_cur_record, 
                                                     new_cur_record_info, 
                                                     tmp_op, ops_to_be_moved, 
                                                     failed_to_write_to_secondary_db, 
                                                     candidate_separate_ops >>

RecheckSecondaryDbRecordIsUpdated(self) == /\ pc[self] = "RecheckSecondaryDbRecordIsUpdated"
                                           /\ IF secondary_db_record["tx_id"] /= fetched_cur_record[self]["prep_tx_id"]
                                                 THEN /\ failed_to_write_to_secondary_db' = [failed_to_write_to_secondary_db EXCEPT ![self] = TRUE]
                                                      /\ UNCHANGED secondary_db_record
                                                 ELSE /\ secondary_db_record' =                        [
                                                                                    tx_id |-> new_cur_record_info[self]["tx_id"],
                                                                                    deleted |-> new_cur_record_info[self]["deleted"],
                                                                                    applied_tx_ids |-> tmp_applied_tx_ids_on_secondary_db[self]
                                                                                ]
                                                      /\ UNCHANGED failed_to_write_to_secondary_db
                                           /\ pc' = [pc EXCEPT ![self] = "UpdateCurrentReplDbRecord"]
                                           /\ UNCHANGED << stored_ops, 
                                                           expected_applied_tx_id_on_secondary_db, 
                                                           repl_db_record, 
                                                           retry_of_enqueue, 
                                                           need_to_stop, 
                                                           fetched_cur_record, 
                                                           new_cur_record_info, 
                                                           tmp_op, 
                                                           ops_to_be_moved, 
                                                           tmp_applied_tx_ids_on_secondary_db, 
                                                           candidate_separate_ops >>

UpdateCurrentReplDbRecord(self) == /\ pc[self] = "UpdateCurrentReplDbRecord"
                                   /\ IF ~failed_to_write_to_secondary_db[self] /\ fetched_cur_record[self]["version"] = repl_db_record["version"]
                                         THEN /\ repl_db_record' =                   [
                                                                       tx_id |-> new_cur_record_info[self]["tx_id"],
                                                                       deleted |-> new_cur_record_info[self]["deleted"],
                                                                       prep_tx_id |-> Null,
                                                                       version |-> fetched_cur_record[self]["version"] + 1,
                                                                   
                                                                       insert_tx_ids |-> fetched_cur_record[self]["insert_tx_ids"] \union ExtractInsertOpTxIds(ops_to_be_moved[self]),
                                                                   
                                                                       ops |-> (candidate_separate_ops[self]["ins_ops"]
                                                                               \union {candidate_separate_ops[self]["non_ins_ops"][x] : x \in DOMAIN candidate_separate_ops[self]["non_ins_ops"]}) \ {Null}
                                                                   ]
                                         ELSE /\ TRUE
                                              /\ UNCHANGED repl_db_record
                                   /\ pc' = [pc EXCEPT ![self] = "Repeat"]
                                   /\ UNCHANGED << stored_ops, 
                                                   expected_applied_tx_id_on_secondary_db, 
                                                   secondary_db_record, 
                                                   retry_of_enqueue, 
                                                   need_to_stop, 
                                                   fetched_cur_record, 
                                                   new_cur_record_info, tmp_op, 
                                                   ops_to_be_moved, 
                                                   tmp_applied_tx_ids_on_secondary_db, 
                                                   failed_to_write_to_secondary_db, 
                                                   candidate_separate_ops >>

RecordWriter(self) == Repeat(self)
                         \/ BuildNextOperationByMergingOperationChain(self)
                         \/ SetPrepTxIdOfReplDbRecord(self)
                         \/ WriteOpsToSecondaryDbRecord(self)
                         \/ RecheckSecondaryDbRecordIsUpdated(self)
                         \/ UpdateCurrentReplDbRecord(self)

(* Allow infinite stuttering to prevent deadlock on termination. *)
Terminating == /\ \A self \in ProcSet: pc[self] = "Done"
               /\ UNCHANGED vars

Next == (\E self \in RecordWriterProcesses: RecordWriter(self))
           \/ Terminating

Spec == /\ Init /\ [][Next]_vars
        /\ \A self \in RecordWriterProcesses : WF_vars(RecordWriter(self))

Termination == <>(\A self \in ProcSet: pc[self] = "Done")

\* END TRANSLATION 

=============================================================================
