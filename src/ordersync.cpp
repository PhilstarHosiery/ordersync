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

#define DIR "/storage/philstar/snapshot/phsystem.latest/"


void generate_item_map(pqxx::work &txn, map<string, int> &m, map<string, int> &m_trim);
string trim(string str);
string flatten_key(string artcono, string color, string size);

int main(int argc, char** argv) {
    
    if (argc != 3) {
        cout << "Usage: ordersync [db.conf] [dbf file]" << endl;
        return 1;
    }
    
    string dbconf(argv[1]);
    string dbffile(argv[2]);
    
    ifstream dbconfin;
    dbconfin.open(dbconf);
    
    string dbstring;
    getline(dbconfin, dbstring);
    
    try {
        // Database Connection & Transaction
        pqxx::connection c(dbstring);
        pqxx::work txn(c);

        map<string, int> m;
        map<string, int> m_trim;
        
        // Build the map from DB
        generate_item_map(txn, m, m_trim);
        
        /*
        for(auto i : m) {
            cout << i.first << " -> " << i.second << endl;
        }
        */

        
        dbfReader reader;
        reader.open(dbffile);
        
        
        string orderno;
        string custvar;
        string artcono;
        string orddate;
        string barcode_id;
        string colorway;
        string size;
        string orderqty;
        string quotaqty;
        
        
        while (reader.next()) {
            /* Columns
             * 011: orderno VARCHAR(15)
             * 012: custvar VARCHAR(3)
             * 013: artcono VARCHAR(8)
             * 015: orddate DATE
             * 023: barcode_id VARCHAR(8)
             * 024: colorway VARCHAR(20)
             * 027: size VARCHAR(12)
             * 030: orderqty NUMERIC(10,2)
             * 031: quotaqty NUMERIC(10,2)
             */
            
            custvar = reader.getString(12);
            orderno = reader.getString(15);
            
            if (reader.isClosedRow()) {
                // cout << "SKIP " << custvar << ", " << orderno << endl;
                continue;
            }
            
            artcono = reader.getString(13);
            colorway = reader.getString(24);
            size = reader.getString(27);
            
            orddate = reader.getString(15);
            orderqty = reader.getString(30);
            
            
            
            if (m[flatten_key(artcono, colorway, size)] == 0) {
                if (m_trim[flatten_key(trim(artcono), trim(colorway), trim(size))] == 0) {
                    if (orderqty != "0.00") {
                        string kniprod = reader.getString(47);
                        
                        if (!kniprod.empty()) { // kniprod
                            cout << " IGNORE NOT FOUND - " << orddate
                                    << " : [" << artcono << "] " << reader.getString(14) << ", " << colorway << ", " << size
                                    << " = " << orderqty << ", " << kniprod << endl;
                        } else {
                            // cout << " IGNORE 0 kniprod: [" << artcono << "] " << reader.getString(14) << ", " << colorway << ", " << size << endl;
                        }
                    } else {
                        // cout << " IGNORE 0 order: [" << artcono << "] " << reader.getString(14) << ", " << colorway << ", " << size << endl;
                    }
                } else {
                    // should be synced, trimmed
                }
            } else {
                // should be synced, not trimmed
            }
            // cout << flatten_key(artcono, colorway, size) << " : " << m[flatten_key(artcono, colorway, size)] << endl;
        }
        
        m.clear();
        m_trim.clear();
        
        reader.close();
        
        
        
        c.disconnect();
        
        
        /*
        dbfReader color_reader;
        string color_fn;
        string artcono;
        string article;
        string size_index;
        string size;
        string color;

        value_type value;

        
        set<string> pass_set;
        map<string, string> size_map;




        
        // Prepare the filename map
        fileFinder artcolor;
        artcolor.openDir(DIR "ARTICLES/ARTCOLOR/");


        // Counters
        int add = 0;
        int del = 0;
        int update = 0;
        int pass = 0;
        int ignore = 0;
        int total = 0;


        // Prepared Statements
        c.prepare("add", "INSERT INTO \"production:sock_item\" (artcono, size_index, article, color, size) VALUES ($1, $2, $3, $4, $5)");
        c.prepare("update_article", "UPDATE \"production:sock_item\" SET article=$1 WHERE id=$2");
        c.prepare("update_size", "UPDATE \"production:sock_item\" SET size=$1 WHERE id=$2");
        c.prepare("del", "DELETE FROM \"production:sock_item\" WHERE id=$1");

        
        // Open articles.dbf and process each article
        reader.open(DIR "articles.DBF");

        while (reader.next()) {
            // Columns
            // 009: artcono VARCHAR(8)
            // 010: article VARCHAR(30)
            // 053: size01 VARCHAR(12)
            // 063: size02 VARCHAR(12)
            // 073: size03 VARCHAR(12)
            // 083: size04 VARCHAR(12)
            // 093: size05 VARCHAR(12)
            // 103: size06 VARCHAR(12)

            if (reader.isClosedRow()) {
                // cout << "SKIP " << artcono << ", " << article << endl;
                continue;
            }
                        
            artcono = reader.getString(9);
            article = reader.getString(10);

            color_fn = artcolor.findFile(artcono + ".dbf");
            if (color_fn.empty()) {
                continue; // skip if no ARTCOLOR file exists
            }

            size_map.clear();
            if (!reader.getString(53).empty()) size_map["1"] = reader.getString(53);
            if (!reader.getString(63).empty()) size_map["2"] = reader.getString(63);
            if (!reader.getString(73).empty()) size_map["3"] = reader.getString(73);
            if (!reader.getString(83).empty()) size_map["4"] = reader.getString(83);
            if (!reader.getString(93).empty()) size_map["5"] = reader.getString(93);
            if (!reader.getString(103).empty()) size_map["6"] = reader.getString(103);

            
            // Open artcolor file and process each color
            color_reader.open(COLOR_DIR + color_fn);

            while (color_reader.next()) {
                if (color_reader.isClosedRow()) {
                    // cout << "SKIP DELETED COLOR: " << color_reader.getString(1) << endl;
                    continue;
                }
                
                color = color_reader.getString(1);

                for (auto itr = size_map.begin(); itr != size_map.end(); ++itr) {
                    size_index = itr->first;
                    size = itr->second;

                    auto it = m.find(flatten_key(artcono, color, size_index));
                    if (it == m.end()) {
                        // not found, add
                        // (artcono, size_index, article, color, size)
                        string flat_key = flatten_key(artcono, color, size_index);
                        
                        if (pass_set.find(flat_key) == pass_set.end()) {
                            cout << " ADD [" << flat_key << "] " << article << " / " << size << endl;
                            txn.prepared("add")(artcono) (size_index) (article) (color) (size).exec();
                            pass_set.insert(flat_key);
                            add++;
                        } else {
                            cout << " SKIP DUPLICATE [" << flat_key << "] " << article << " / " << size << endl;
                            ignore++;
                        }
                    } else {
                        // found, if same remove, else update
                        value = it->second;

                        bool updated = false;

                        if (article != value.article) {
                            cout << " UPDATE [" << flatten_key(artcono, color, size_index) << "] ARTICLE: " << value.article << " -> " << article << endl;
                            txn.prepared("update_article")(article) (value.id).exec();
                            updated = true;
                        }

                        if (size != value.size) {
                            cout << " UPDATE [" << flatten_key(artcono, color, size_index) << "] SIZE: " << value.size << " -> " << size << endl;
                            txn.prepared("update_size")(size) (value.id).exec();
                            updated = true;
                        }

                        pass_set.insert(it->first);
                        m.erase(it);
                        // cout << " PASS [" << flatten_key(artcono, color, size_index) << "] " << article << " / " << size << endl;

                        if (updated) {
                            update++;
                        } else {
                            pass++;
                        }
                    }

                    total++;
                }
            }
        }

        for (auto itr = m.begin(); itr != m.end(); ++itr) {
            string key = itr->first;
            value_type value = itr->second;

            cout << " DELETE [" << key << "] --> " << value.id << ", " << value.article << ", " << value.size << endl;
            txn.prepared("del")(value.id).exec();
            del++;
        }

        txn.commit();

        cout << pass << " rows passed" << endl;
        cout << update << " rows updated" << endl;
        cout << add << " rows added" << endl;
        cout << del << " rows deleted" << endl;
        cout << ignore << " rows ignored (duplicates)" << endl;
        cout << total << " rows total" << endl;

        
         * 
         * */

    } catch (const pqxx::pqxx_exception &e) {
        cerr << "pqxx_exception: " << e.base().what() << endl;
    } catch (const std::exception &e) {
        cerr << "exception: " << e.what() << endl;
        return 1;
    }

    return 0;
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

    // txn.commit();
}

// trims paranthesis and all blanks
string trim(string str) {
    int l = 0;
    string tmp = "";
    
    for(int i = 0 ; i < str.length() ; i++) {
        if (str[i] == '(') l++;
        else if (str[i] == ')') l--;
        else if(l == 0) {
            tmp += str[i];
        }
    }
    
    boost::trim(tmp);
    
//    if (str.compare(tmp) != 0) {
//        cout << "[" << str << "] -> [" << tmp << "]" << endl;
//    }
    
    return tmp;
}

string flatten_key(string artcono, string color, string size) {
    return artcono + "|||" + color + "|||" + size;
}
