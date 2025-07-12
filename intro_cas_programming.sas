/* This code shows the basic structures in CAS Programming */

/* Create a cas session called 'mysess' with 30 mins timeout */
cas mysess sessopts=(caslib=casuser timeout=1800 locale="en_US" metrics=True);

/* Shows notes, warnings, and errors,plus informational messages
that can help you understand more about SAS's internal processing
(e.g., index usage, optimizer decisions, etc.)
*/
options msglevel=i;

/* Automatically assigns a library reference to each existing caslib in
   the session. It uses the caslib name as the library reference name.
   If the caslib name does not follow SAS naming rules, a libref is
   not assigned.
*/
caslib _all_ assign;

/* Load and replace 'sashelp.cars' to 'casuser.mycars' etc. */
proc casutil;
  droptable incaslib=casuser casdata="MYcaRs" quiet;
  load data=sashelp.cars outcaslib="casuser" casout="mycars" replace;
quit;

/* Create a libref to the respective caslibs.
This is to allow traditional SAS compute engine to interact with CAS.
*/
libname mycas cas caslib=casuser;

/* Filter and create new calculated column - traditional data step */
proc cas;
  mytable={caslib="casuser", name="mycars_discount"};
  source myds;
    data casuser.mycars_discount;
      set casuser.mycars;
      discount=msrp*.90;
      where origin='Europe';
      keep make model msrp discount;
    run;
  endsource;

  /* Use the runcode */
  datastep.runcode / code=myds;

  /* View top 7 observations */
  table.fetch / table=mytable, to=7;
quit;

/* ---------- Filter and save a CAS table ---------- */
proc cas;
  mytable={caslib="casuser", name="mycars",
           where="Make='Audi' and MSRP>35000"
           };
  /* Obtaining number of rows */
    simple.numRows result=nr / table=mytable;
    totrow=nr.numrows;

  /* Display the table */
    table.fetch result=rf /
        table=mytable,
        to=totrow,
        index=FALSE;
  /* Write result to a table, 'myaudi' */
  myaudi=rf.fetch;

  /* Save the result table to in-memory table, "myaudi" */
  saveresult myaudi caslib="casuser" casout="myaudi";

/* ---------- Transposing a CAS table ---------- */
/* tranpose.transpose to transpose 'casuser.myaudi.
   Transpose from long to wide.
*/
proc cas;
  intable={caslib="casuser", name="myaudi",groupby={"make"}};
  outtable={caslib="casuser", name="myaudi_transposed"};

  /* Perform the transpose */
  transpose.transpose /
           table=intable,
           casout=outtable || {replace=True},
           attributes={{name="make", label='Car Maker'}},
           transpose={"msrp","invoice"},
           id={"model"},
           prefix="Audi_";
  table.fetch / table=outtable, index=FALSE;
  table.columnInfo / table=outtable;
quit;


/* ---------- Cleaning existing CAS tables ---------- */

/* Delete any leftover tables and start over.
   If you have more tables in your personal 'casuser' caslib to remove,
   feel free to plug into the list in mytables
 */
proc cas;
  mytables={"mycars", "mycars_discount", "myaudi", "myaudi_transposed"};
  do i over mytables;
    table.tableExists result=tbl / caslib="casuser", name=upcase(i);
    if tbl.exists>0 then do;
      table.dropTable / caslib="casuser", name=upcase(i);
    end;
  end;
  table.tableInfo / caslib="casuser";
quit;

/* Obtaining Table/file level info */
proc cas;
 mylib="casuser";
 table.tableInfo / caslib=mylib; /* CAS Table info */
 table.fileInfo / caslib=mylib; /* filesystem level info */
quit;


/* Terminate the current CAS session called 'mysess' */
cas mysess terminate;
