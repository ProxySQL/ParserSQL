typedef struct SelectStmt
{
    NodeTag type;
    List *targetList;
    bool distinct;
} SelectStmt;
