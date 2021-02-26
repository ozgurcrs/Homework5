using Dapper;
using DapperWebApplication.Model;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Threading.Tasks;

namespace DapperWebApplication.Controllers
{
    [ApiController]
    [Route("api/[controller]/[action]")]
    public class DapperController : ControllerBase
    {
        private readonly IConfiguration _configuration; 
        public DapperController(IConfiguration configuration)
        {
            _configuration = configuration;
        }
        [HttpGet]
        // Tekil bir kullanıcı ekleme işlemi yapıyoruz.
        // Burada sql cümlesinde values kısmında @ başlayan alanlarımızla objemizin parametreleri birebir aynı olmak zorundadır. Dapper bu şekilde anlıyor.
        public IActionResult InsertSingleUser()
        {
            using (IDbConnection db = new SqlConnection(_configuration.GetConnectionString("DefaultConnection")))
            {
                if(db.State != ConnectionState.Open)
                {
                    db.Open();
                    string sql = "Insert Into TestDB (FirstName,SurName,Age) Values(@FirstName,@Surname,@Age);";
                    var affected = db.Execute(sql, new
                    {
                        FirstName = "Mehmet",
                        SurName="Kuzukumru",
                        Age=24
                    });
                    return Ok("Success");
                }
            }
            return BadRequest("Hata");
        }
        [HttpGet]

        // Bir obje üzerinden tablomuza toplu veri ekleme işlemi yapabiliyoruz. db.Execute 2.parametresini obje olarak verebiliyoruz.
        // her bir objeyi okuduktan sonra tekrar tekrar sorgumuzu her bir obje elemanı için çalıştırıyor.
        public IActionResult InsertUserFromList()
        {
            using (IDbConnection db = new SqlConnection(_configuration.GetConnectionString("DefaultConnection")))
            {
                if (db.State != ConnectionState.Open)
                {
                    db.Open();
                    string sql = "Insert Into TestDB (FirstName,SurName,Age) Values(@FirstName,@Surname,@Age);";
                    object[] userList = new object[10]; 

                    for(var i =0; i<10; i++)
                    {
                        userList[i] = new
                        {
                            FirstName = "Özgür-" + i,
                            SurName = "Varlıksız-" + i,
                            Age = i + 18
                        };
                    }
                    var affected = db.Execute(sql, userList);
                    return Ok("Success");
                }
            }
            return BadRequest("Hata");
        }

        [HttpGet]

        //  Obje üzerinden bir update işlemi yapıyor. new [] { new {} } Şeklinde kullansaydık çoklu bir update yapabilirdik. 
        public IActionResult UpdateUser()
        {
            using (IDbConnection db = new SqlConnection(_configuration.GetConnectionString("DefaultConnection")))
            {
                if (db.State != ConnectionState.Open)
                {
                    db.Open();
                    string sql = "Update TestDB Set Age = @Age where ID = @ID";
                    var affected = db.Execute(sql, new { ID = 2, Age = 15 });
                    return Ok("Success");
                }
            }
            return BadRequest("Hata");
        }

        [HttpGet]

        // Bu sefer tekli göndermek yerine toplu bir delete işlemi yaptık. new {} objecleri okurken tekrar tekrar sorgumuzu her bir obje elemanı için çalıştırıyor.
        public IActionResult DeleteUser()
        {
            using (IDbConnection db = new SqlConnection(_configuration.GetConnectionString("DefaultConnection")))
            {
                if (db.State != ConnectionState.Open)
                {
                    db.Open();
                    string sql = "Delete From TestDB where ID = @ID";
                    var affected = db.Execute(sql, new[] { 
                        new { ID = 3},
                        new { ID = 10}
                    });
                    return Ok("Success");
                }
            }
            return BadRequest("Hata");
        }

        [HttpGet]

        // Execute reader db üzerinden blok data okuyabiliyor.
        public IActionResult GetAllDataFromUser()
        {
            using (IDbConnection db = new SqlConnection(_configuration.GetConnectionString("DefaultConnection")))
            {
                if (db.State != ConnectionState.Open)
                {
                    db.Open();
                    string sql = "SELECT * FROM TestDB";
                    var reader = db.ExecuteReader(sql);
                    DataTable dataTable = new DataTable();
                    dataTable.Load(reader);
                   
                    return Ok("Success");
                }
            }
            return BadRequest("Hata");
        }

        [HttpGet]

        // Db'den tek bir kolon okumak istiyorsak, ExecuteScaler kullanıyoruz. İlk satır ilk kolunu döner
        public IActionResult GetUserCount()
        {
            using (IDbConnection db = new SqlConnection(_configuration.GetConnectionString("DefaultConnection")))
            {
                if (db.State != ConnectionState.Open)
                {
                    db.Open();
                    string sql = "SELECT Count(*) FROM TestDB";
                    var userCount = db.ExecuteScalar<int>(sql);
                    return Ok(userCount);
                }
            }
            return BadRequest("Hata");
        }

        [HttpGet]
        // Query kullanarak bir modelimize database tablomuzu bind edebiliyoruz.
        public IActionResult GetUsersQueryMap()
        {
            using (IDbConnection db = new SqlConnection(_configuration.GetConnectionString("DefaultConnection")))
            {
                if (db.State != ConnectionState.Open)
                {
                    db.Open();
                    string sql = "SELECT * FROM TestDB";
                    IEnumerable<UserModel> userModel = db.Query<UserModel>(sql);
                    return Ok("Success");
                }
            }
            return BadRequest("Hata");
        }

        [HttpGet]
        // Dynamic olarak resultset alıp map ediyor.
        public IActionResult GetDynamicMap()
        {
            using (IDbConnection db = new SqlConnection(_configuration.GetConnectionString("DefaultConnection")))
            {
                if (db.State != ConnectionState.Open)
                {
                    db.Open();
                    string sql = "SELECT * FROM TestDB";
                    var user = db.Query(sql);
                    return Ok("Success");
                }
            }
            return BadRequest("Hata");
        }

        [HttpGet]
        // Query ile modelimiz üzerinden oluşturulmuş bir veriyi database üzerinden silebiliyoruz.
        public IActionResult DeleteQuery()
        {
            using (IDbConnection db = new SqlConnection(_configuration.GetConnectionString("DefaultConnection")))
            {
                if (db.State != ConnectionState.Open)
                {
                    db.Open();
                    string sql = @"Delete from TestDB where ID = @ID;";
                    IEnumerable<UserModel> userModel = db.Query<UserModel>(sql, new { ID = 13 });
                    return Ok("Success");
                }
            }
            return BadRequest("Hata");
        }


        [HttpGet]
        // QueryMultiple kullanarak tek bir connection ile 2 sorgu birden gönderebildim.
        public IActionResult MultipleQuery()
        {
            using (IDbConnection db = new SqlConnection(_configuration.GetConnectionString("DefaultConnection")))
            {
                if (db.State != ConnectionState.Open)
                {
                    db.Open();
                    string sql = @"SELECT * FROM TestDB where ID = @ID;
                                   SELECT * FROM SocialTest where ID = @ID";
                    var multipleQuery = db.QueryMultiple(sql, new { ID = 1 });
                    var userInformation = multipleQuery.Read<UserModel>();
                    var userDetail = multipleQuery.Read<UserDetail>().ToList();
                    return Ok("Success");
                }
            }
            return BadRequest("Hata");
        }


        [HttpGet]
        // 2 Tablomuza kesin olarak eklemek istediğimiz bir yapımız olduğunu düşünerek sırasıyla veri ekleme işlemi yaptık.
        // Burada sırasıyla gidildiğinde ilk sorgu bitse bile ikinci sorgunun bitmesini bekliyor. Sql serverda select sorgusu attığımızda verileri dönmüyor.
        // Commit olduktan sonra her şey normale dönüyor. // Commitlemeden öncekiler veritabanına yansımaz.
        public IActionResult Transaction()
        {
            using (IDbConnection db = new SqlConnection(_configuration.GetConnectionString("DefaultConnection")))
            {
                if (db.State != ConnectionState.Open)
                {
                    db.Open();

                    using (var transaction = db.BeginTransaction()){
                        string sql =@"Insert Into TestDB (FirstName,SurName,Age) Values(@FirstName,@Surname,@Age);";
                        var affected = db.Execute(sql, new
                        {
                            FirstName = "Mehmet",
                            SurName = "Kuzukumru",
                            Age = 24
                        },transaction);

                        sql = @"Insert Into SocialTest (Facebook,Twitter,Instagram) values (@Facebook,@Twitter,@Instagram);";

                        UserDetail userDetail = new UserDetail()
                        {
                            Facebook = "ozgurcrs",
                            Twitter = "",
                            Instagram = "ozgurcrs"
                        };

                        affected = db.Execute(sql,userDetail,transaction);
                        transaction.Commit();

                        return Ok("Success");
                    }
                }
                   
            }
            return BadRequest("Hata");
        }

        [HttpGet]
        // one to one birebir ilişkili tabloyu bir model içine bind ettik.
        public IActionResult OneToOne()
        {
            using (IDbConnection db = new SqlConnection(_configuration.GetConnectionString("DefaultConnection")))
            {
                if (db.State != ConnectionState.Open)
                {
                    db.Open();
                    string sql = @"SELECT * FROM TestDB inner join SocialTest on TestDB.ID = SocialTest.ID";
                    var result = db.Query<UserModel, UserDetail, UserModel>(sql, (user, userD) =>
                    {
                        return user;
                    }
                    );
                    return Ok(result);
                }
            }
            return BadRequest("Hata");
        }

        [HttpGet]
        // one to many birden çok yorumu olan bir tabloyu birbirine bağladık ve bir model içine bind ettik.
        public IActionResult OneToMany()
        {
            using (IDbConnection db = new SqlConnection(_configuration.GetConnectionString("DefaultConnection")))
            {
                if (db.State != ConnectionState.Open)
                {
                    db.Open();
                    string sql = @"SELECT * FROM TestDB inner join TestComment on TestDB.ID = TestComment.ID";
                    var result = db.Query<UserModel, UserComment, UserModel>(sql, (user, comment) =>
                    {
                        return user;
                    }
                    );
                    return Ok(result);
                }
            }
            return BadRequest("Hata");
        }



    }
}
