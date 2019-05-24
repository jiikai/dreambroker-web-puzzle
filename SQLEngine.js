/*
    A mock SQL Engine, written as a solution to
    Dream Broker Code Challenge Web Puzzle #5.
*/

/* eslint-disable no-console */

"use strict";

/* Test Code */

var dummyDatabase = {
    employees: [{
        id: 1,
        name: "Alice",
        phone: "12345678",
    },
    {
        id: 2,
        name: "Bob",
        phone: "87654321",
    }
    ],
    monsters: [{
        id: 1,
        name: "Loch Ness Monster",
        home: "Loch Ness, Scotland"
    },
    {
        id: 6,
        name: "Yeti",
        home: "Himalaya Mountains"
    }
    ]
};

(() => {
    let engine = new SQLEngine(dummyDatabase);

    console.log(engine.execute("SELECT employees.name FROM employees"));
    console.log(engine.execute(`SELECT employees.id, employees.name
                                FROM employees WHERE employees.id = 1`));
    console.log(engine.execute(`SELECT monsters.id, monsters.name, monsters.home
                            FROM monsters WHERE monsters.home = 'Himalaya Mountains'`));
    console.log(engine.execute(`SELECT monsters.name, monsters.home
                            FROM monsters WHERE monsters.id = employees.id`));
})();

/* SQL Engine supporting SELECT with optional WHERE. */

function SQLEngine(database) {

    /*! Polyfill for Object.fromEntries(), use if not implemented natively.

        https://gist.github.com/friendlyanon/c4a157982673a1a0b543b60bd4b9bcf7
    */

    if (!Object.fromEntries) {
        Object.defineProperty(Object, "fromEntries", {
            value: function fromEntries(iterable) {
                const obj = {};
                for (const pair of iterable) {
                    if (typeof pair !== "object" || pair === null) {
                        throw new TypeError("iterable for fromEntries should yield objects");
                    }
                    obj[pair[0]] = pair[1];
                }
                return obj;
            },
            writeable: true,
            configurable: true
        });
    }

    this.database   = database;
    this.hasTable   = tbl => this.database.hasOwnProperty(tbl);
    this.hasColumn  = (tbl, col) => this.hasTable(tbl)
                        && this.database[tbl][0].hasOwnProperty(col);

    this.whereFunctionConstructor = cond => Function("db", "cmpRow", `return
        db["${cond.leftTable}"][cmpRow]["${cond.leftOperand}"] ${cond.operator}
        ${cond.rightTable ?
        `db["${cond.rightTable}"][cmpRow]["${cond.rightOperand}"]`
        : `${cond.rightOperand}`};`);

    this.whereComponents = cond => {
        if (cond.length < 3)
            return false;
        const operatorIdx = cond.search(/(<=)|(>=)|[>=<]/);
        if (operatorIdx <= 0) // Invalid operator or lack of left operand
            return false;
        const leftExpr = cond.substring(0, operatorIdx).trim().split(/\./);
        if (leftExpr.length !== 2)
            return false;

        let rightExpr = cond.substring(cond.substring(operatorIdx)
                            .search(/\w|['"]/) + operatorIdx);
        const lengthIfSemicolon = str =>
            (str.charAt(str.length - 1) === ";" ? str.length - 1 : str.length);
        let rightOperand, rightTable = null;
        if (/['"]/.test(rightExpr) && !/['"]/.test(rightExpr.substring(1)))
            return false;
        else if (/\d/.test(rightExpr) && !/[\d, ]+([.]?[\d])*/.test(rightExpr))
            return false;
        else {
            rightExpr = rightExpr.trim().split(".");
            if (rightExpr.length !== 2)
                return false;
            rightTable  = rightExpr[0];
        }
        rightOperand = rightTable
                        ? rightExpr[1]
                            .substring(0, lengthIfSemicolon(rightExpr[1]))
                        : rightExpr.substring(0, lengthIfSemicolon(rightExpr))
                            .trim();
        return {
            operator:       cond.charAt(operatorIdx) == "="
                            ? "==" : cond.charAt(operatorIdx),
            leftOperand:    leftExpr[1],
            leftTable:      leftExpr[0],
            rightOperand:   rightOperand,
            rightTable:     rightTable
        };
    };

    this.selectComponents = query => {
        if (!query.startsWith("select"))
            return false;
        const fromIdx = query.indexOf("from");
        if (fromIdx === -1 || fromIdx < 9)
            return false;
        let parts = query.substring(7).split(/\s*from\s*|\s*where\s*/);
        let where = null;
        if (parts.length === 3) {
            where = this.whereComponents(parts[2]);
            if (!where)
                return false;
        }
        return {
            cols: parts[0].trim().split(/\s*,\s*/),
            tbls: parts[1].trim().split(/\s*,\s*/),
            cond: where
        };
    };

    this.associate = (tbls, cols) => {
        let tblToCol = new Map();
        tbls.forEach(tbl => tblToCol.set(tbl, new Array()));
        cols.forEach(val => {
            const dotIdx = val.indexOf(".");
            const tbl = val.substring(0, dotIdx),
                col = val.substring(dotIdx + 1);
            if (!this.hasColumn(tbl, col))
                return false;
            const tblIdx = tbls.findIndex(e => e === tbl);
            if (tblIdx === -1)
                return false;
            if (!this.hasTable(tbl))
                return false;
            tblToCol.get(tbl).push(col);
        });
        return tblToCol;
    };

    this.queryTable = (tbl, tblToCol, where) =>
        this.database[tbl].filter((_row, index) =>
            (where ? where(this.database, index) : true))
        .map(row => (Object.fromEntries(Object.entries(row)
            .filter(entry => tblToCol.get(tbl).includes(entry[0]))
            .map(([k, v]) => ([`${tbl}.${k}`, v])))));

    this.normalize = query => {
        let beginQuote = query.search(/['"]/);
        if (beginQuote === -1)
            return query.toLowerCase();
        else {
            let endQuote = beginQuote + query.substring(beginQuote + 1)
                            .search(/['"]/);
            if (endQuote === -1)
                return null;
            else
                return query.substring(0, beginQuote).toLowerCase()
                    + query.substring(beginQuote, endQuote + 1)
                    + query.substring(endQuote + 1).toLowerCase();
        }
    };

    this.execute = query => {
        const components = this.selectComponents(this.normalize(query.trim()));
        if (!components)
            throw Error("Invalid syntax");
        let whereFn = components.cond
                    ? this.whereFunctionConstructor(components.cond) : null,
            tbls = components.tbls,
            cols = components.cols;

        let tblColMap = this.associate(tbls, components.cols);
        if (!tblColMap)
            throw Error("Nonexistent columns or tables");
        let results = this.queryTable(tbls[0], tblColMap, whereFn);
        if (tbls.length > 1)
            tbls.forEach(tbl =>
                this.queryTable(tbl).forEach((row, index) =>
                    Object.entries(row).forEach(([k, v]) =>
                        Object.fromEntries(Object.entries(results[index])
                            .splice(cols.findIndex(col => col === k),
                                0, ([k, v]))))));
        return results;
    };

}
