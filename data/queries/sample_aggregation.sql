select u.usr_id, u.usr_name, u.contact, t.trans_id, t.trans_date from test.user u inner join test.transaction t on u.usr_id = t.usr_id;