#include <iostream>
#include <fstream>
#include <cstdlib>
#include <pqxx/pqxx>
#include <cryptopp/sha.h>
#include <sstream>
#include <iomanip>
#include <set>
#include <boost/algorithm/string.hpp>

using namespace std;

#include "dbfReader.h"

struct order {
    /*
      id serial NOT NULL,
      date date NOT NULL,
      customer character varying(64) NOT NULL,
      orderno character varying(64) NOT NULL,
      item_id integer NOT NULL,
      quantity integer NOT NULL,
      quota integer NOT NULL,
     */
    int id;
    string date;
    string customer;
    string orderno;
    int item_id;
    int quantity;
    int quota;
    string barcode_id;
};

int sync(pqxx::work &txn, map<string, order> &m, order ord);
void generate_order_map(pqxx::work &txn, map<string, order> &m, set<string> &s);
void generate_item_map(pqxx::work &txn, map<string, int> &m, map<string, int> &m_trim);
string trim(string str);
string flatten_key(string artcono, string color, string size);
string getKey(order ord);
string isoDate(string date);

int main(int argc, char** argv) {

    if (argc != 3) {
        cout << "Usage: ordersync [db.conf] [dbf file]" << endl;
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
        set<string> sBarcodeId;
        map<string, order> orderMap;

        // Build the maps from DB
        generate_item_map(txn, m, mTrim);
        generate_order_map(txn, orderMap, sBarcodeId);

        // Stats
        int total = 0;
        int zeroorder = 0;
        int zeroproduction = 0;
        int ignore = 0;
        int guess = 0;
        int found = 0;
        
        // Stats
        int total_pre = orderMap.size();
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

        // Field variables
        string closechk;
        string pantychk;
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

        // SQL prepared statements
        c.prepare("add", "INSERT INTO \"production:order\" (date, customer, orderno, item_id, quantity, quota, barcode_id) VALUES ($1, $2, $3, $4, $5, $6, $7)");
        c.prepare("update_date", "UPDATE \"production:order\" SET date=$1 WHERE id=$2");
        c.prepare("update_customer", "UPDATE \"production:order\" SET customer=$1 WHERE id=$2");
        c.prepare("update_orderno", "UPDATE \"production:order\" SET orderno=$1 WHERE id=$2");
        c.prepare("update_item_id", "UPDATE \"production:order\" SET item_id=$1 WHERE id=$2");
        c.prepare("update_quantity", "UPDATE \"production:order\" SET quantity=$1 WHERE id=$2");
        c.prepare("update_quota", "UPDATE \"production:order\" SET quota=$1 WHERE id=$2");
        c.prepare("del", "DELETE FROM \"production:order\" WHERE id=$1");

        // Temporaries
        int item;
        order tmpOrder;
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
            kniprod = reader.getString(kniprodIdx);

            artcono = reader.getString(artconoIdx);
            colorway = reader.getString(colorwayIdx);
            size = reader.getString(sizeIdx);

            orddate = reader.getString(orddateIdx);
            orderqty = reader.getString(orderqtyIdx);
            quotaqty = reader.getString(quotaqtyIdx);

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

            if (closechk == "T" && kniprod.empty()) {
                continue;
            }

            // Find item id, and sync accordingly
            item = m[flatten_key(artcono, colorway, size)];
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

                    syncState = sync(txn, orderMap, tmpOrder);
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

                syncState = sync(txn, orderMap, tmpOrder);
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
        for (auto itr = orderMap.begin(); itr != orderMap.end(); itr++) {
            cout << " DELETE " << itr->first << endl;
            txn.prepared("del")(itr->second.id).exec();
            del++;
        }
        
        // Release orderMap
        orderMap.clear();
        
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

        cout << "Stats (order Synchronization)" << endl
                << " Total Initial = " << total_pre << endl
                << " Passed        = " << pass << endl
                << " Updated       = " << update << endl
                << " Inserted  (+) = " << insert << endl
                << " Deleted   (-) = " << del << endl
                << " Total Final   = " << total_post << endl;
                
        // Close DB connection
        c.disconnect();

    } catch (const pqxx::pqxx_exception &e) {
        cerr << "pqxx_exception: " << e.base().what() << endl;
        return 1;
    } catch (const std::exception &e) {
        cerr << "exception: " << e.what() << endl;
        return 1;
    }

    return 0;
}

// Synchronization
// return: -1 if new insert, 0+ for number of updates (0 means found without update, ie pass)
int sync(pqxx::work &txn, map<string, order> &m, order ord) {
    //        c.prepare("add", "INSERT INTO \"production:order\" (date, customer, orderno, item_id, quantity, quota) VALUES ($1, $2, $3, $4, $5, $6)");
    //        c.prepare("update_date", "UPDATE \"production:order\" SET date=$1 WHERE id=$2");
    //        c.prepare("update_customer", "UPDATE \"production:order\" SET customer=$1 WHERE id=$2");
    //        c.prepare("update_orderno", "UPDATE \"production:order\" SET orderno=$1 WHERE id=$2");
    //        c.prepare("update_item_id", "UPDATE \"production:order\" SET item_id=$1 WHERE id=$2");
    //        c.prepare("update_quantity", "UPDATE \"production:order\" SET quantity=$1 WHERE id=$2");
    //        c.prepare("update_quota", "UPDATE \"production:order\" SET quota=$1 WHERE id=$2");

    auto itr = m.find(ord.barcode_id);
    order ordm;
    int rtn = 0;

    if (itr != m.end()) {
        ordm = itr->second;

        if (ordm.date != ord.date) {
            cout << " UPDATE " << itr->first << " date at " << ordm.id << " : " << ordm.date << " -> " << ord.date << endl;
            txn.prepared("update_date")(ord.date)(ordm.id).exec();
	        rtn++;
        }

        if (ordm.customer != ord.customer) {
            cout << " UPDATE " << itr->first << " customer at " << ordm.id << " : " << ordm.customer << " -> " << ord.customer << endl;
            txn.prepared("update_customer")(ord.customer)(ordm.id).exec();
	        rtn++;
        }

        if (ordm.orderno != ord.orderno) {
            cout << " UPDATE " << itr->first << " orderno at " << ordm.id << " : " << ordm.orderno << " -> " << ord.orderno << endl;
            txn.prepared("update_orderno")(ord.orderno)(ordm.id).exec();
	        rtn++;
        }

        if (ordm.item_id != ord.item_id) {
            cout << " UPDATE " << itr->first << " item_id at " << ordm.id << " : " << ordm.item_id << " -> " << ord.item_id << endl;
            txn.prepared("update_item_id")(ord.item_id)(ordm.id).exec();
	        rtn++;
        }

        if (ordm.quantity != ord.quantity) {
            cout << " UPDATE " << itr->first << " quantity at " << ordm.id << " : " << ordm.quantity << " -> " << ord.quantity << endl;
            txn.prepared("update_quantity")(ord.quantity)(ordm.id).exec();
	        rtn++;
        }

        if (ordm.quota != ord.quota) {
            cout << " UPDATE " << itr->first << " quota at " << ordm.id << " : " << ordm.quota << " -> " << ord.quota << endl;
            txn.prepared("update_quota")(ord.quota)(ordm.id).exec();
	        rtn++;
        }

        m.erase(itr);
    } else {
        cout << " NOT FOUND: INSERT " << getKey(ord) << endl;
        txn.prepared("add")(ord.date)(ord.customer)(ord.orderno)(ord.item_id)(ord.quantity)(ord.quota)(ord.barcode_id).exec();
	    rtn = -1;
    }

    return rtn;
}

void generate_order_map(pqxx::work &txn, map<string, order> &m, set<string> &s) {
    pqxx::result r = txn.exec("SELECT * FROM \"production:order\"");
    string barcode_id;

    for (auto i = 0; i != r.size(); ++i) {
        order tmp;

        r[i]["id"].to(tmp.id);
        r[i]["date"].to(tmp.date);
        r[i]["customer"].to(tmp.customer);
        r[i]["orderno"].to(tmp.orderno);
        r[i]["item_id"].to(tmp.item_id);
        r[i]["quantity"].to(tmp.quantity);
        r[i]["quota"].to(tmp.quota);
        r[i]["barcode_id"].to(barcode_id);

        m[barcode_id] = tmp;
        s.insert(barcode_id);
    }
}

void generate_item_map(pqxx::work &txn, map<string, int> &m, map<string, int> &m_trim) {
    pqxx::result r = txn.exec("SELECT * FROM \"production:sock_item\" ORDER BY artcono, color, size");

    for (pqxx::result::size_type i = 0; i != r.size(); ++i) {
        string artcono;
        string color;
        string size;
        int itemId;

        r[i]["artcono"].to(artcono);
        r[i]["color"].to(color);
        r[i]["size"].to(size);

        r[i]["id"].to(itemId);

        // cout << key.art << ", " << key.col << ", " << key.siz << " --> " << value.art << ", " << value.col << ", " << value.siz << endl;
        m[flatten_key(artcono, color, size)] = itemId;
        m_trim[flatten_key(trim(artcono), trim(color), trim(size))] = itemId;
    }
}

// trims parenthesis and all blanks
string trim(string str) {
    int l = 0;
    string tmp = "";

    for (int i = 0; i < str.length(); i++) {
        if (str[i] == '(') l++;
        else if (str[i] == ')') l--;
        else if (l == 0) {
            tmp += str[i];
        }
    }

    boost::trim(tmp);

    return tmp;
}

string flatten_key(string artcono, string color, string size) {
    return artcono + "|||" + color + "|||" + size;
}

string getKey(order ord) {
    return flatten_key(ord.customer, ord.orderno, to_string(ord.item_id));
}

string isoDate(string date) {
    return date.substr(0, 4) + "-" + date.substr(4, 2) + "-" + date.substr(6, 2);
}
