%%
select_stmt:
    SELECT target_list
    {
        $$ = make_select($2);
    }
  | SELECT DISTINCT target_list
    {
        $$ = make_distinct_select($3);
    }
;
%%
