#include <iostream>
#include <fstream>
#include <cstdlib>
#include <pqxx/pqxx>
#include <cryptopp/sha.h>
#include <sstream>
#include <iomanip>
#include <set>

using namespace std;

#include "dbfReader.h"

struct order_content {
    /*
        id serial NOT NULL,
        date date NOT NULL,
        customer character varying(64) NOT NULL,
        orderno character varying(64) NOT NULL,
        item_id integer NOT NULL,
        quantity integer NOT NULL,
        quota integer NOT NULL,
        barcode_id character varying(8) NOT NULL,
        exfdate date,
     */
    int id;
    int item_id;
    int quantity;
    int quota;
    string barcode_id;
    string exfdate;
    int order_id;
    bool closed;

    // to be removed later...
    string date;
    string customer;
    string orderno;

};

struct order {
    /*
        id integer NOT NULL DEFAULT nextval('"production:order_id_seq1"'::regclass),
        name character varying(128) NOT NULL,
        customer character varying(128) NOT NULL,
        subclass character varying(128) NOT NULL,
        date date NOT NULL,
        order_group_id integer,
     */
    int id;
    string name;
    string customer;
    // string subclass;
    string date;
};

int sync(pqxx::work &txn, map<string, order_content> &m, order_content ord);
int syncAndFindOrderId(pqxx::work &txn, map<string, order> &m, string name, string customer, string date);

void generate_order_map(pqxx::work &txn, map<string, order> &m);
void generate_order_content_map(pqxx::work &txn, map<string, order_content> &m, set<string> &s);
void generate_item_map(pqxx::work &txn, map<string, int> &m, map<string, int> &m_trim);

string trim(string str);
string flatten_key(string artcono, string color, string size);
string getKey(order_content ord);
string isoDate(string date);

int main(int argc, char** argv) {
    if (argc != 3) {
        cout << "Usage: ordersync [db.conf] [prosheet.dbf file]" << endl;
        return 1;
    }

    // Parameters
    // 1 - configuration filename
    // 2 - prosheet.DBF location
    string dbconf(argv[1]);
    string dbffile(argv[2]);

    // Get dbstring from 1st parameter
    ifstream dbconfin;
    dbconfin.open(dbconf);
    string dbstring;
    getline(dbconfin, dbstring);

    try {
        // Database connection & transaction
        pqxx::connection c(dbstring);
        pqxx::work txn(c);

        // Maps and sets
        map<string, int> m;
        map<string, int> mTrim;
        map<string, order> orderMap;
        set<string> sBarcodeId;
        map<string, order_content> orderContentMap;

        // Build the maps from DB
        generate_item_map(txn, m, mTrim);
        generate_order_map(txn, orderMap);
        generate_order_content_map(txn, orderContentMap, sBarcodeId);

        // Stats
        int total = 0;
        int zeroorder = 0;
        int zeroproduction = 0;
        int ignore = 0;
        int guess = 0;
        int found = 0;

        // Stats
        int total_pre = orderContentMap.size();
        int pass = 0;
        int update = 0;
        int insert = 0;
        int del = 0;
        int total_post = 0;

        // Prepare for FoxPro DBF reading...
        dbfReader reader;
        reader.open(dbffile);

        // Find field indices
        int closechkIdx = reader.getFieldIndex("closechk");
        int pantychkIdx = reader.getFieldIndex("pantychk");
        int yconlyIdx = reader.getFieldIndex("yconly");
        int ordernoIdx = reader.getFieldIndex("orderno");
        int custvarIdx = reader.getFieldIndex("custvar");
        int artconoIdx = reader.getFieldIndex("artcono");
        int articleIdx = reader.getFieldIndex("article");
        int orddateIdx = reader.getFieldIndex("orddate");
        int barcode_idIdx = reader.getFieldIndex("barcode_id");
        int colorwayIdx = reader.getFieldIndex("colorway");
        int sizeIdx = reader.getFieldIndex("size");
        int orderqtyIdx = reader.getFieldIndex("orderqty");
        int quotaqtyIdx = reader.getFieldIndex("quotaqty");
        int kniprodIdx = reader.getFieldIndex("kniprod");
        int exfdateIdx = reader.getFieldIndex("exfdate");

        // Field variables
        string closechk;
        string pantychk;
        string yconly;
        string kniprod;

        string orderno;
        string custvar;
        string artcono;
        string orddate;
        string barcode_id;
        string colorway;
        string size;
        string orderqty;
        string quotaqty;
        string exfdate;

        // SQL prepared statements
        c.prepare("add", "INSERT INTO production.order_content (date, customer, orderno, item_id, quantity, quota, barcode_id, exfdate, order_id) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)");
        c.prepare("update_date", "UPDATE production.order_content SET date=$1 WHERE id=$2");
        c.prepare("update_customer", "UPDATE production.order_content SET customer=$1 WHERE id=$2");
        c.prepare("update_orderno", "UPDATE production.order_content SET orderno=$1 WHERE id=$2");
        c.prepare("update_item_id", "UPDATE production.order_content SET item_id=$1 WHERE id=$2");
        c.prepare("update_quantity", "UPDATE production.order_content SET quantity=$1 WHERE id=$2");
        c.prepare("update_quota", "UPDATE production.order_content SET quota=$1 WHERE id=$2");
        c.prepare("update_exfdate", "UPDATE production.order_content SET exfdate=$1 WHERE id=$2");
        c.prepare("update_order_id", "UPDATE production.order_content SET order_id=$1 WHERE id=$2");
        c.prepare("update_closed", "UPDATE production.order_content SET closed=$1 WHERE id=$2");
        c.prepare("del", "DELETE FROM production.order_content WHERE id=$1");

        // Temporaries
        int item;
        int order_id;
        order_content tmpOrder;
        int syncState;

        // Loop through the items in prosheet.DBF
        while (reader.next()) {

            if (reader.isClosedRow()) { // Skip closed rows
                continue;
            }

            custvar = reader.getString(custvarIdx);
            orderno = reader.getString(ordernoIdx);
            barcode_id = reader.getString(barcode_idIdx);

            closechk = reader.getString(closechkIdx);
            pantychk = reader.getString(pantychkIdx);
            yconly = reader.getString(yconlyIdx);
            kniprod = reader.getString(kniprodIdx);

            artcono = reader.getString(artconoIdx);
            colorway = reader.getString(colorwayIdx);
            size = reader.getString(sizeIdx);

            orddate = reader.getString(orddateIdx);
            orderqty = reader.getString(orderqtyIdx);
            quotaqty = reader.getString(quotaqtyIdx);
            exfdate = reader.getString(exfdateIdx);

            // Skip invalid rows
            if (orderqty == "") { // orderqty should be present.
                continue;
            }

            if (quotaqty == "") { // quotaqty should be present.
                continue;
            }

            if (orddate == "") {
                continue;
            }

            if (pantychk == "T") {
                continue;
            }

            if (yconly == "T") {
                continue;
            }

            if (closechk == "T" && kniprod.empty()) {
                continue;
            }

            if (reader.getString(articleIdx) == "3642") {
                if (colorway == "BLACK") colorway = "0005 BLACK";
                else if (colorway == "NAVY") colorway = "0004 NAVY";
                else if (colorway == "39 CARAMEL MEL") colorway = "0039 CARAMEL MEL";
                else if (colorway == "33 DENIM MEL.(SS18)") colorway = "0033 DENIM MEL.(SS18)";
                else if (colorway == "41 TRUE RED(SS18)") colorway = "0041 TRUE RED(SS18)";
                else if (colorway == "81 OLYM. BLUE(SS18)") colorway = "0081 OLYM. BLUE(SS18)";
            }

            // Find item id, and sync accordingly
            item = m[flatten_key(artcono, colorway, size)];
            order_id = syncAndFindOrderId(txn, orderMap, orderno, custvar, orddate);

            if (item == 0) {
                item = mTrim[flatten_key(trim(artcono), trim(colorway), trim(size))];
                if (item == 0) {
                    if (orderqty != "0.00" /* && orderqty != "" */) {
                        if (!kniprod.empty()) { // kniprod
                            cout << " IGNORE NOT FOUND - " << orddate
                                    << " : [" << artcono << "] " << reader.getString(articleIdx) << ", " << colorway << ", " << size
                                    << " = " << orderqty << ", " << kniprod << endl;

                            ignore++;
                        } else {
                            // cout << " IGNORE 0 kniprod: [" << artcono << "] " << reader.getString(articleIdx) << ", " << colorway << ", " << size << endl;
                            zeroproduction++;
                        }
                    } else {
                        // cout << " IGNORE 0 order: [" << artcono << "] " << reader.getString(articleIdx) << ", " << colorway << ", " << size << endl;
                        zeroorder++;
                    }
                } else {
                    // should be synced, trimmed
                    tmpOrder.customer = custvar;
                    tmpOrder.date = isoDate(orddate);
                    tmpOrder.item_id = item;
                    tmpOrder.orderno = orderno;
                    tmpOrder.quantity = stoi(orderqty);
                    tmpOrder.quota = stoi(quotaqty);
                    tmpOrder.barcode_id = barcode_id;
                    tmpOrder.exfdate = isoDate(exfdate);
                    tmpOrder.order_id = order_id;
                    tmpOrder.closed = (closechk == "T");

                    syncState = sync(txn, orderContentMap, tmpOrder);
                    if (syncState < 0) {
                        insert++;
                    } else if (syncState == 0) {
                        pass++;
                    } else {
                        update++;
                    }

                    guess++;
                }
            } else {
                // should be synced, not trimmed
                // if current row is in orderMap, check each item. if diff, update. remove from orderMap
                // if not in orderMap, insert into DB.
                tmpOrder.customer = custvar;
                tmpOrder.date = isoDate(orddate);
                tmpOrder.item_id = item;
                tmpOrder.orderno = orderno;
                tmpOrder.quantity = stoi(orderqty);
                tmpOrder.quota = stoi(quotaqty);
                tmpOrder.barcode_id = barcode_id;
                tmpOrder.exfdate = isoDate(exfdate);
                tmpOrder.order_id = order_id;
                tmpOrder.closed = (closechk == "T");

                syncState = sync(txn, orderContentMap, tmpOrder);
                if (syncState < 0) {
                    insert++;
                } else if (syncState == 0) {
                    pass++;
                } else {
                    update++;
                }

                found++;
            }

            total++;
        }

        // Release resources
        m.clear();
        mTrim.clear();
        reader.close();
        sBarcodeId.clear();

        // if orderMap not empty, remove from DB
        for (auto itr = orderContentMap.begin(); itr != orderContentMap.end(); itr++) {
            cout << " DELETE " << itr->first << endl;
            txn.exec_prepared("del", itr->second.id);
            del++;
        }

        // Release orderMap
        orderContentMap.clear();

        // Commit changes made to SQL
        txn.commit();

        // Display statistics
        cout << "Stats (sock_item Identification)" << endl
                << " Total             = " << total << endl
                << " Found             = " << found << " (" << found * 100.0 / total << "%)" << endl
                << " Guessed           = " << guess << " (" << guess * 100.0 / total << "%)" << endl
                << " Ignored 0 Prod    = " << zeroproduction << " (" << zeroproduction * 100.0 / total << "%)" << endl
                << " Ignored 0 Order   = " << zeroorder << " (" << zeroorder * 100.0 / total << "%)" << endl
                << " Ignored Not Found = " << ignore << " (" << ignore * 100.0 / total << "%)" << endl;

        // Stats
        //        int total_pre = 0;
        //        int pass = 0;
        //        int update = 0;
        //        int insert = 0;
        //        int del = 0;
        //        int total_post = 0;
        total_post = total_pre + insert - del;

        if (update == 0 && insert == 0 && del == 0) {
            cout << "Stats (order Synchronization)" << endl
                << " Total           = " << total_pre << endl;
        } else {
            cout << "Stats (order Synchronization)" << endl
                << " Total Initial   = " << total_pre << endl
                << " Passed          = " << pass << endl
                << " Updated         = " << update << endl
                << " Inserted    (+) = " << insert << endl
                << " Deleted     (-) = " << del << endl
                << " Total Final     = " << total_post << endl;
        }

        // Close DB connection
        // c.disconnect();

    } catch (const pqxx::sql_error &e) {
        cerr << "sql_error: " << e.what() << " / " << e.query() << endl;
        return 1;
    } catch (const std::exception &e) {
        cerr << "exception: " << e.what() << endl;
        return 1;
    }

    return 0;
}

// Synchronization
// return: -1 if new insert, 0+ for number of updates (0 means found without update, ie pass)

int sync(pqxx::work &txn, map<string, order_content> &m, order_content ord) {
    //        c.prepare("add", "INSERT INTO production.order_content (date, customer, orderno, item_id, quantity, quota) VALUES ($1, $2, $3, $4, $5, $6)");
    //        c.prepare("update_date", "UPDATE production.order_content SET date=$1 WHERE id=$2");
    //        c.prepare("update_customer", "UPDATE production.order_content SET customer=$1 WHERE id=$2");
    //        c.prepare("update_orderno", "UPDATE production.order_content SET orderno=$1 WHERE id=$2");
    //        c.prepare("update_item_id", "UPDATE production.order_content SET item_id=$1 WHERE id=$2");
    //        c.prepare("update_quantity", "UPDATE production.order_content SET quantity=$1 WHERE id=$2");
    //        c.prepare("update_quota", "UPDATE production.order_content SET quota=$1 WHERE id=$2");

    auto itr = m.find(ord.barcode_id);
    order_content ordm;
    int rtn = 0;

    if (itr != m.end()) {
        ordm = itr->second;

        if (ordm.date != ord.date) {
            cout << " UPDATE " << itr->first << " date at " << ordm.id << " : " << ordm.date << " -> " << ord.date << endl;
            txn.exec_prepared("update_date", ord.date, ordm.id);
            rtn++;
        }

        if (ordm.customer != ord.customer) {
            cout << " UPDATE " << itr->first << " customer at " << ordm.id << " : " << ordm.customer << " -> " << ord.customer << endl;
            txn.exec_prepared("update_customer", ord.customer, ordm.id);
            rtn++;
        }

        if (ordm.orderno != ord.orderno) {
            cout << " UPDATE " << itr->first << " orderno at " << ordm.id << " : " << ordm.orderno << " -> " << ord.orderno << endl;
            txn.exec_prepared("update_orderno", ord.orderno, ordm.id);
            rtn++;
        }

        if (ordm.item_id != ord.item_id) {
            cout << " UPDATE " << itr->first << " item_id at " << ordm.id << " : " << ordm.item_id << " -> " << ord.item_id << endl;
            txn.exec_prepared("update_item_id", ord.item_id, ordm.id);
            rtn++;
        }

        if (ordm.quantity != ord.quantity) {
            cout << " UPDATE " << itr->first << " quantity at " << ordm.id << " : " << ordm.quantity << " -> " << ord.quantity << endl;
            txn.exec_prepared("update_quantity", ord.quantity, ordm.id);
            rtn++;
        }

        if (ordm.quota != ord.quota) {
            cout << " UPDATE " << itr->first << " quota at " << ordm.id << " : " << ordm.quota << " -> " << ord.quota << endl;
            txn.exec_prepared("update_quota", ord.quota, ordm.id);
            rtn++;
        }

        if (ordm.exfdate != ord.exfdate) {
            cout << " UPDATE " << itr->first << " exfdate at " << ordm.id << " : " << ordm.exfdate << " -> " << ord.exfdate << endl;
            txn.exec_prepared("update_exfdate", ord.exfdate, ordm.id);
            rtn++;
        }

        if (ordm.order_id != ord.order_id) {
            cout << " UPDATE " << itr->first << " order_id at " << ordm.id << " : " << ordm.order_id << " -> " << ord.order_id << endl;
            txn.exec_prepared("update_order_id", ord.order_id, ordm.id);
            rtn++;
        }

        if (ordm.closed != ord.closed) {
            cout << " UPDATE " << itr->first << " closed at " << ordm.id << " : " << ordm.closed << " -> " << ord.closed << endl;
            txn.exec_prepared("update_closed", ord.closed, ordm.id);
            rtn++;
        }

        m.erase(itr);
    } else {
        cout << " NOT FOUND: INSERT " << getKey(ord) << endl;
        txn.exec_prepared("add", ord.date, ord.customer, ord.orderno, ord.item_id, ord.quantity, ord.quota, ord.barcode_id, ord.exfdate, ord.order_id);
        rtn = -1;
    }

    return rtn;
}

int syncAndFindOrderId(pqxx::work &txn, map<string, order> &m, string name, string customer, string date) {
    // if any information different, warn the user, but resume with "name"
    // if exists, return id
    // else insert and return the id

    string isodate = isoDate(date);

    auto order = m.find(name);
    if (order == m.end()) {
        // insert new order entry in DB
        txn.conn().prepare("add_order", "INSERT INTO production.order (name, customer, subclass, date) VALUES ($1, $2, '', $3) RETURNING id");
        pqxx::result r = txn.exec_prepared("add_order", name, customer, isodate);

        if (r.size() != 1) { // if above query returns non-1 results, something is wrong.
            return -1;
        }

        int id;
        r[0]["id"].to(id);

        // Create an order and insert into the order map
        struct order tmp;
        tmp.id = id;
        tmp.name = name;
        tmp.customer = customer;
        tmp.date = isodate;

        m[name] = tmp;

        return id;

    } else {
        if (order->second.name != name || order->second.customer != customer) {
            cout << " (" << name << ", " << customer << ", " << isodate << ") != existing ("
                    << order->second.name << ", " << order->second.customer << ", " << order->second.date << "), ignore and using initial values." << endl;
        } else if (order->second.date != isodate) {
            // Order date is meaningless at this point. Kepp the output clean.
            // cout << " Order date for [" << name << ", " << customer << "] (" << isodate << ") != existing ("
            //         << order->second.date << "), ignore and using initial date." << endl;
        }

        return order->second.id;
    }


}

void generate_order_map(pqxx::work &txn, map<string, order> &m) {
    pqxx::result r = txn.exec("SELECT * FROM production.order");

    for (auto i = 0; i != r.size(); ++i) {
        order tmp;

        r[i]["id"].to(tmp.id);
        r[i]["name"].to(tmp.name);
        r[i]["customer"].to(tmp.customer);
        r[i]["date"].to(tmp.date);

        m[tmp.name] = tmp;
    }
}

void generate_order_content_map(pqxx::work &txn, map<string, order_content> &m, set<string> &s) {
    pqxx::result r = txn.exec("SELECT * FROM production.order_content");
    string barcode_id;

    for (auto i = 0; i != r.size(); ++i) {
        order_content tmp;

        r[i]["id"].to(tmp.id);
        r[i]["date"].to(tmp.date);
        r[i]["customer"].to(tmp.customer);
        r[i]["orderno"].to(tmp.orderno);
        r[i]["item_id"].to(tmp.item_id);
        r[i]["quantity"].to(tmp.quantity);
        r[i]["quota"].to(tmp.quota);
        r[i]["barcode_id"].to(barcode_id);
        r[i]["exfdate"].to(tmp.exfdate);
        r[i]["order_id"].to(tmp.order_id);
        r[i]["closed"].to(tmp.closed);

        m[barcode_id] = tmp;
        s.insert(barcode_id);
    }
}

void generate_item_map(pqxx::work &txn, map<string, int> &m, map<string, int> &m_trim) {
    // pqxx::result r = txn.exec("SELECT production.sock_item.item_id, production.sock_article.artcono, production.sock_color.name as color, production.sock_size.name as size FROM production.sock_article, production.sock_color, production.sock_size, production.sock_item WHERE production.sock_item.article_id = production.sock_article.article_id AND production.sock_item.color_id = production.sock_color.color_id AND production.sock_item.size_id = production.sock_size.size_id");
    pqxx::result r = txn.exec("SELECT sock_item_detail.item_id, sock_item_detail.artcono, sock_item_detail.color, sock_item_detail.size FROM production.sock_item_detail;");

    for (pqxx::result::size_type i = 0; i != r.size(); ++i) {
        string artcono;
        string color;
        string size;
        int itemId;

        r[i]["artcono"].to(artcono);
        r[i]["color"].to(color);
        r[i]["size"].to(size);

        r[i]["item_id"].to(itemId);

        // cout << artcono << ", " << color << ", " << size << " --> " << value.art << ", " << value.col << ", " << value.siz << endl;
        m[flatten_key(artcono, color, size)] = itemId;
        m_trim[flatten_key(trim(artcono), trim(color), trim(size))] = itemId;
    }
}

// trims parenthesis and all blanks

string trim(string str) {

    // First trim paranthesis contents -> tmp
    int l = 0;
    string tmp = "";

    for (int i = 0; i < str.length(); i++) {
        if (str[i] == '(') l++;
        else if (str[i] == ')') l--;
        else if (l == 0) {
            tmp += str[i];
        }
    }


    // Find first and last non-space character position, return the substr.
    bool began = false;
    int first = 0;
    int last = 0;

    for (int i = 0; i < tmp.length(); i++) {
        if (!isspace(tmp[i])) {
            if (began) {
                last = i;
            } else {
                first = i;
                last = i;
                began = true;
            }
        }
    }

    return tmp.substr(first, last - first + 1);
}

string flatten_key(string artcono, string color, string size) {
    return artcono + "|||" + color + "|||" + size;
}

string getKey(order_content ord) {
    return flatten_key(ord.customer, ord.orderno, to_string(ord.item_id));
}

string isoDate(string date) {
    return date.substr(0, 4) + "-" + date.substr(4, 2) + "-" + date.substr(6, 2);
}
