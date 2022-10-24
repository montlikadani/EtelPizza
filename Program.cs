using MySql.Data.MySqlClient;
using System;

namespace DbTests {
    class Program {

        private static MySqlConnection Connection;

        static void Main(string[] args) {
            try {
                (Connection = new MySqlConnection() {
                    ConnectionString = "server=localhost;uid=root;database=pizza;"
                }).Open();

                Console.WriteLine("Adatbázishoz csatlakozva.\n");
            } catch (MySqlException ex) {
                Console.WriteLine(ex.Message);
                Console.ReadKey();
                return;
            }

            Console.WriteLine("23. feladat: Hány házhoz szállítása volt az egyes futároknak?");
            Console.WriteLine("fnev\tHázhoz szállítás");

            ReadFromSql("select fnev, count(rendeles.fazon) 'Házhoz szállítás' from `futar`, `rendeles` where rendeles.fazon = futar.fazon group by fnev;", reader => {
                Console.WriteLine($"{reader.GetString("fnev")}\t{reader.GetInt32(1)}");
                return 1;
            });

            Console.WriteLine("\n24. feladat: A fogyasztás alapján mi a pizzák népszerűségi sorrendje?");
            Console.WriteLine("pnev\tNépszerűség");

            ReadFromSql("select pizza.pnev, sum(db) 'Népszerűség' FROM `pizza`, `tetel` where tetel.pazon = pizza.pazon group by pizza.pnev order by sum(db) desc;", reader => {
                Console.WriteLine($"{reader.GetString(0)}\t{reader.GetInt32(1)}");
                return 1;
            });

            Console.WriteLine("\n25. feladat: A rendelések összértéke alapján mi a vevők sorrendje?");
            Console.WriteLine("pnev\tVevő sorrend");

            ReadFromSql("select pizza.pnev, sum(par * db) 'Vevő sorrend' from `vevo`, `tetel`, `rendeles`, `pizza` where tetel.razon = rendeles.razon and rendeles.vazon = vevo.vazon and tetel.pazon = pizza.pazon group by pizza.pnev order by sum(par * db) desc;",
                reader => {
                    Console.WriteLine($"{reader.GetString(0)}\t{reader.GetInt32(1)}");
                    return 1;
                });

            Console.WriteLine("\n26. feladat: Melyik a legdrágább pizza?");
            Console.WriteLine("pazon\tpnev\tpar");

            ReadFromSql("select pazon, pnev, par 'Legdrágább pizza' from pizza where par = (select max(par) from pizza);", reader => {
                Console.WriteLine($"{reader.GetString(0)}\t{reader.GetString(1)}\t{reader.GetInt32(2)}");
                return 1;
            });

            Console.WriteLine("\n27. feladat: Ki szállította házhoz a legtöbb pizzát?");
            Console.WriteLine("fnev\tfazon\tLegtöbb pizza szállítás");

            ReadFromSql("select futar.fnev, futar.fazon, count(*) 'Legtöbb pizza szállítás' from rendeles, futar where rendeles.fazon = (select min(rendeles.fazon) from rendeles);",
                reader => {
                    Console.WriteLine($"{reader.GetString(0)}\t{reader.GetInt32(1)}\t{reader.GetInt32(2)}");
                    return 1;
                });

            Console.WriteLine("\n28. feladat: Ki ette a legtöbb pizzát?");
            Console.WriteLine("vnev\tLegtöbb pizza evés");

            ReadFromSql("select vevo.vnev, count(*) 'Legtöbb pizza evés' from rendeles join vevo on vevo.vazon = rendeles.vazon where rendeles.vazon = (select min(rendeles.vazon) from rendeles);",
                reader => {
                    Console.WriteLine($"{reader.GetString(0)}\t{reader.GetInt32(1)}");
                    return 1;
                });

            Connection.Close();
            Console.ReadKey();
        }

        private static void ReadFromSql(string command, Func<MySqlDataReader, int> func) {
            using (MySqlDataReader reader = new MySqlCommand(command, Connection).ExecuteReader()) {
                while (reader.Read()) {
                    func.Invoke(reader);
                }
            }
        }
    }
}
