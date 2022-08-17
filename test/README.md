


## 测试用例

<table>
    <!-- header -->
    <tr>
        <th>类别</th>
        <th>用例</th>
        <th>场景</th>
        <th>备注</th>
    </tr>
    <!-- body -->
    <!-- produce cases -->
    <tr align="left">
        <th rowspan="2">Produce</th><th >send 1msg (Ready)</th><th></th><th>Passed</th>
    </tr>
    <tr align="left"><th>sendAsync 1msg (Ready) </th><th></th><th>Passed</th></tr>
    <tr align="left">
        <th rowspan="10">Produce-Check<br>(Sync)</th><th >check 1msg goto Discard</th><th></th><th>Passed</th>
    </tr>
    <tr align="left"><th>check 1msg goto Dead</th><th></th><th>Passed</th></tr>
    <tr align="left"><th>check 1msg goto Pending</th><th></th><th>Passed</th></tr>
    <tr align="left"><th>check 1msg goto Blocking</th><th></th><th>Passed</th></tr>
    <tr align="left"><th>check 1msg goto Retrying</th><th></th><th>Passed</th></tr>
    <tr align="left"><th>check 1msg goto Upgrade</th><th></th><th>Passed</th></tr>
    <tr align="left"><th>check 1msg goto Degrade</th><th></th><th>Passed</th></tr>
    <tr align="left"><th>check 1msg goto Shift</th><th></th><th>Passed</th></tr>
    <tr align="left"><th>check 1msg goto Transfer (L1)</th><th></th><th>Passed</th></tr>
    <tr align="left"><th>check 8msg goto ALL goto actions</th><th></th><th>Passed</th></tr>
    <tr align="left">
        <th rowspan="10">Produce-Check<br>(Async)</th><th >check 1msg goto Discard</th><th></th><th>Passed</th>
    </tr>
    <tr align="left"><th>check 1msg goto Dead</th><th></th><th>Passed</th></tr>
    <tr align="left"><th>check 1msg goto Pending</th><th></th><th>Passed</th></tr>
    <tr align="left"><th>check 1msg goto Blocking</th><th></th><th>Passed</th></tr>
    <tr align="left"><th>check 1msg goto Retrying</th><th></th><th>Passed</th></tr>
    <tr align="left"><th>check 1msg goto Upgrade</th><th></th><th>Passed</th></tr>
    <tr align="left"><th>check 1msg goto Degrade</th><th></th><th>Passed</th></tr>
    <tr align="left"><th>check 1msg goto Shift</th><th></th><th>Passed</th></tr>
    <tr align="left"><th>check 1msg goto Transfer (L1)</th><th></th><th>Passed</th></tr>
    <tr align="left"><th>check 8msg goto ALL goto actions</th><th></th><th>Passed</th></tr>
<tr align="left">
        <th rowspan="2">Produce-Overall</th><th >send 2msg (Ready) + check 1msg (Transfer to L1)</th><th></th><th>Passed</th>
    </tr>
    <tr align="left"><th>sendAsync 2msg (Ready) + check 1msg (Transfer to L1) </th><th></th><th>Passed</th></tr>
    <!-- listen cases -->
    <tr align="left">
        <th rowspan="14">Listen</th></th><th>listen 1msg (Ready)</th><th></th><th>Passed</th>
    </tr>
    <tr align="left"><th>listen 1msg (Pending)</th><th></th><th>Passed</th></tr>
    <tr align="left"><th>listen 1msg (Blocking)</th><th></th><th>Passed</th></tr>
    <tr align="left"><th>listen 1msg (Retrying)</th><th></th><th>Passed</th></tr>
    <tr align="left"><th>listen 4msg from ALL status</th><th></th><th>Passed</th></tr>
    <tr align="left"><th>listen 1msg from L2</th><th></th><th>Passed</th></tr>
    <tr align="left"><th>listen 2msg from L1(Ready) and L2</th><th></th><th>Passed</th></tr>
    <tr align="left"><th>listen 1msg from B1</th><th></th><th>Passed</th></tr>
    <tr align="left"><th>listen 2msg from L1(Ready) and B1</th><th></th><th>Passed</th></tr>
    <tr align="left"><th>listen 1msg from S1</th><th></th><th>Passed</th></tr>
    <tr align="left"><th>listen 2msg from L1(Ready) and S1</th><th></th><th>Passed</th></tr>
    <tr align="left"><th>listen 4msg from L1(Ready) and L2&B1&S1</th><th></th><th>Passed</th></tr>
    <tr align="left"><th>listen 7msg from All levels</th><th></th><th>Passed</th></tr>
    <!-- before check cases -->
    <tr align="left">
        <th rowspan="9">Before-Check</th></th><th>check goto Discard 1msg</th><th></th><th>Passed</th>
    </tr>
    <tr align="left"><th>check goto Dead 1msg</th><th></th><th>Passed</th></tr>
    <tr align="left"><th>check goto Pending 1msg</th><th></th><th>Passed</th></tr>
    <tr align="left"><th>check goto Blocking 1msg</th><th></th><th>Passed</th></tr>
    <tr align="left"><th>check goto Retrying 1msg</th><th></th><th>Passed</th></tr>
    <tr align="left"><th>check goto Upgrade 1msg</th><th></th><th>Passed</th></tr>
    <tr align="left"><th>check goto Degrade 1msg</th><th></th><th>Passed</th></tr>
    <tr align="left"><th>check goto Shift 1msg</th><th></th><th>Passed</th></tr>
    <tr align="left"><th>check goto Transfer 1msg</th><th></th><th>Passed</th></tr>
    <tr align="left"><th>check goto ALL goto-actions 9msg</th><th></th><th>Passed</th></tr>
    <!-- handle cases -->
    <tr align="left">
        <th rowspan="10">Handle</th></th><th>handle goto Done 1msg</th><th></th><th>Passed</th>
    </tr>
    <tr align="left"><th>handle goto Discard 1msg</th><th></th><th>Passed</th></tr>
    <tr align="left"><th>handle goto Dead 1msg</th><th></th><th>Passed</th></tr>
    <tr align="left"><th>handle goto Pending 1msg</th><th></th><th>Passed</th></tr>
    <tr align="left"><th>handle goto Blocking 1msg</th><th></th><th>Passed</th></tr>
    <tr align="left"><th>handle goto Retrying 1msg</th><th></th><th>Passed</th></tr>
    <tr align="left"><th>handle goto Upgrade 1msg</th><th></th><th>Passed</th></tr>
    <tr align="left"><th>handle goto Degrade 1msg</th><th></th><th>Passed</th></tr>
    <tr align="left"><th>handle goto Shift 1msg</th><th></th><th>Passed</th></tr>
    <tr align="left"><th>handle goto ALL goto-actions 8msg</th><th></th><th>Passed</th></tr>
    <!-- after check cases -->
    <tr align="left">
        <th rowspan="10">After-Check</th></th>
        <th>check goto Discard 1msg</th><th></th><th>Passed</th>
    </tr>
    <tr align="left"><th>check goto Dead 1msg</th><th></th><th>Passed</th></tr>
    <tr align="left"><th>check goto Pending 1msg</th><th></th><th>Passed</th></tr>
    <tr align="left"><th>check goto Blocking 1msg</th><th></th><th>Passed</th></tr>
    <tr align="left"><th>check goto Retrying 1msg</th><th></th><th>Passed</th></tr>
    <tr align="left"><th>check goto Upgrade 1msg</th><th></th><th>Passed</th></tr>
    <tr align="left"><th>check goto Degrade 1msg</th><th></th><th>Passed</th></tr>
    <tr align="left"><th>check goto Shift 1msg</th><th></th><th>Passed</th></tr>
    <tr align="left"><th>check goto Transfer 1msg</th><th></th><th>Passed</th></tr>
    <tr align="left"><th>check goto ALL goto-actions 9msg</th><th></th><th>Passed</th></tr>
    <!-- Status balance cases -->
    <tr align="left">
        <th rowspan="4">Status-Balance</th></th>
        <th>balance status messages: 50(Ready) + 30(Retrying)</th><th></th><th>Passed</th>
    </tr>
    <tr align="left"><th>balance status messages: 50(Ready) + 15(Pending)</th><th></th><th>Passed</th></tr>
    <tr align="left"><th>balance status messages: 50(Ready) + 5(Blocking)</th><th></th><th>Passed</th></tr>
    <tr align="left"><th>balance status messages: ALL 50(Ready) + <br>30(Retrying) + 15(Pending) + 5(Blocking)</th><th></th><th>Passed</th></tr>
    <!-- Level balance cases -->
    <tr align="left">
        <th rowspan="5">Level-Balance</th></th>
        <th>balance level messages: 50(L1) + 30(B1)</th><th></th><th>Passed</th>
    </tr>
    <tr align="left"><th>balance level messages: 50(L1) + 80(L2)</th><th></th><th>Passed</th></tr>
    <tr align="left"><th>balance level messages: 50(L1) + 100(S1)</th><th></th><th>Passed</th></tr>
    <tr align="left"><th>balance status messages: 50(L1) + 60(L2) +<br> 25(B1) + 100(S1)</th><th></th><th>Passed</th></tr>
    <tr align="left"><th>balance status messages: ALL 200(S2) + 100(S1) +<br> L3(70) + 60(L2) + 50(L1) + 30(B1) + 25(B2)</th><th></th><th>Passed</th></tr>
    <!-- Equality-Nursed cases -->
    <tr align="left">
        <th rowspan="5">Equality/Isolation</th></th>
        <th>1000(Normal) + 10000(Radical); <br>rate=100qps; <br>goto Retrying if exceeds the rate; <br>Each normal msg is followed by 10 radical ones;</th><th></th><th>; <br>rate is used-based</th>
    </tr>
    <tr align="left"><th>1000(Normal) + 10000(Radical); <br>rate=100qps; <br>goto Retrying if exceeds the rate; <br>Each normal msg is followed by 10 radical ones;</th><th></th><th>; <br>rate is used-based</th></tr>
    <tr align="left"><th>1000(Normal) + 10000(Radical); <br>rate=100qps; <br>goto Retrying if exceeds the rate; <br>Each normal msg is followed by 10 radical ones;</th><th></th><th>; <br>rate is used-based</th></tr>
    <tr align="left"><th>1000(Normal) + 10000(Radical); <br>quota=5000; <br>goto Blocking if exceeds the rate; <br>Increase quota to 10000 after backlog is empty; <br>Each normal msg is followed by 10 radical ones;</th><th></th><th>rate is used-based</th></tr>
    <tr align="left"><th>1000(Normal) + 10000(Radical); <br>quota=5000; <br>goto Blocking if exceeds the rate; <br>goto DLQ if exceeds max retries; <br>Each normal msg is followed by 10 radical ones;</th><th></th><th>rate is used-based</th></tr>
    <!-- Overall cases -->
    <tr align="left">
        <th rowspan="2">Overall</th></th>
        <th>1000(Normal) + 10000(Radical); <br>rate=100qps; <br>goto Retrying if exceeds the rate; <br>Degrade to B1 If retries meets 5; <br> <br>Each normal msg is followed by 10 radical ones;</th><th></th><th>rate is used-based</th>
    </tr>
    <tr align="left"><th>1000(Normal) + 10000(Radical) + 10000(Radical-2); <br>rate=100qps; <br>goto Retrying if exceeds the rate; <br>Degrade Radical to B1 If retries meets 5; <br>Upgrade Radical-2 to L2 if exceeds the rate; <br>Each normal msg is followed by 10 radical ones;</th><th></th><th>rate is used-based</th></tr>



</table>

