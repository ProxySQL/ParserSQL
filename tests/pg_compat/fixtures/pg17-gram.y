%%
select_stmt:
    SELECT target_list
    {
        $$ = make_select($2);
    }
;
%%
