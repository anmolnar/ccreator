using System;
using System.Collections;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics;
using System.ServiceProcess;
using System.Text;
using System.Timers;
using System.Net.Mail;
using System.IO;
using System.Threading;
using System.Management;
using System.Text.RegularExpressions;

namespace CCreator
{
  public partial class CCreator : ServiceBase
  {
    private readonly string ClassName = "CCreator";

    private string server;
    private string sqlstr;

    private string confsmtp;
    private string confsmtpuser;
    private string confsmtppass;
    private string confver;
    private string senders;

    private System.Timers.Timer t;

    public CCreator()
    {
      InitializeComponent();
    }

    protected override void OnStart(string[] args)
    {
      bool sleep_req = false;
      string line;
      string sqldb = "";
      string sqluser = "";
      string sqlpass = "";

      // TODO: Add code here to start your service.
      try
      {
        try
        {
          StreamReader inifile = new StreamReader(System.Environment.CurrentDirectory + "\\mailed.ini", new System.Text.UTF8Encoding());
          line = inifile.ReadLine();
          server = line.Substring(10);
          line = inifile.ReadLine();
          sqldb = line.Substring(6);
          line = inifile.ReadLine();
          sqluser = line.Substring(8);
          line = inifile.ReadLine();
          sqlpass = line.Substring(8);
          line = inifile.ReadLine();
          confsmtp = line.Substring(5);
          line = inifile.ReadLine();
          confsmtpuser = line.Substring(9);
          line = inifile.ReadLine();
          confsmtppass = line.Substring(9);
          line = inifile.ReadLine();
          confver = line.Substring(8);
          line = inifile.ReadLine();
          senders = line.Substring(8);
          inifile.Close();
          inifile.Dispose();
        }
        catch (Exception ex)
        {
          EventLog.WriteEntry("Ini read error: ", ex.ToString(), EventLogEntryType.Error);
        }

        sqlstr = "data source = " + server + "; initial catalog = " + sqldb + "; user id = " + sqluser + "; password = " + sqlpass;
        do
        {
          if (sleep_req)
          {
            System.Threading.Thread.Sleep(15 * 1000);
            sleep_req = false;
          }
          try
          {
            SqlConnection sqlcon = new SqlConnection(sqlstr);
            sqlcon.Open();
            sqlcon.Close();
            sqlcon.Dispose();
          }
          catch (SqlException sqle)
          {
            EventLog.WriteEntry("SQL server error: " + sqle.Message.ToString().Substring(0, 50) + "...", "OnStart: " + sqle.Message.ToString(), EventLogEntryType.Warning);
            EventLog.WriteEntry("Sleeping for 15 seconds...", "OnStart");
            sleep_req = true;
          }

        } while (sleep_req);

        SendMsg(string.Format("Connected to SQL server: {0}", sqlstr));

        t = new System.Timers.Timer(1000);
        t.Elapsed += new ElapsedEventHandler(t_Elapsed);
        t.AutoReset = true;
        t.Start();
      }
      catch (Exception e)
      {
        //ReportError(e);
        EventLog.WriteEntry("Service start error: ", e.ToString(), EventLogEntryType.Error);
      }
    }

    protected override void OnStop()
    {
      // TODO: Add code here to perform any tear-down necessary to stop your service.
      t.Stop();
      t.Dispose();
      GC.Collect();
      GC.WaitForPendingFinalizers();
      GC.Collect();
    }

    void t_Elapsed(object sender, ElapsedEventArgs e)
    {
      //throw new Exception("The method or operation is not implemented.");
      try
      {
        t.Stop();

        Thread thr = new Thread(CreateSending);
        thr.IsBackground = true;
        thr.Name = "Creator thread";
        thr.Start();
        thr.Join();

        GC.Collect();
        GC.WaitForPendingFinalizers();
        GC.Collect();

        t.Start();
      }
      catch (Exception the)
      {
        ReportError(the);
      }
    }

    private void CreateSending()
    {
      try
      {
        using (SqlConnection sqlcon = new SqlConnection(sqlstr))
        {
          SqlCommand sqlcom = new SqlCommand(sqlstr, sqlcon);
          sqlcom.CommandTimeout = 600;
          SqlDataAdapter sqldata = new SqlDataAdapter();
          DataSet ds = new DataSet();
          sqldata.SelectCommand = sqlcom;

          //leszedjuk a beallitasokat
          sqlcom.CommandText = "select admin_topcreate, admin_localization, admin_mailedtxtroot, "
                             + " (SELECT top 1 snd_sentto from sending where (snd_active = 1 And snd_sent = 0) "
                             + " order by snd_id) as snd_sentto from admins where admin_id=1 ";

          if (ds.Tables.Contains("settings")) { ds.Tables["settings"].Clear(); }
          sqldata.Fill(ds, "settings");

          //ha ugyanannyi van mint ami a topnum, akkor ki kell vonni 1-et, ha nincs mit kikuldeni, akkor meg ne kerjuk le az adatbazist
          int topnum;

          if (ds.Tables["settings"].Rows[0]["snd_sentto"] != DBNull.Value)
          {
            if (Convert.ToInt64(ds.Tables["settings"].Rows[0]["snd_sentto"]) == Convert.ToInt64(ds.Tables["settings"].Rows[0]["admin_topcreate"]))
            {
              topnum = Convert.ToInt32(ds.Tables["settings"].Rows[0]["admin_topcreate"]) - 1;
            }
            else
            {
              topnum = Convert.ToInt32(ds.Tables["settings"].Rows[0]["admin_topcreate"]);
            }
          }
          else
          {
            topnum = 0;
          }

          //ha van melo, leszedjuk a melot
          if (topnum != 0)
          {
            try
            {
              SendMsg("Van meló, leszedjük a melót");

              sqlcom.CommandText = "SELECT DISTINCT top " + topnum.ToString() + " snd_id, mem_email, mem_id, mem_id2, mem_name,"
                             + " (select top 1 sndbuff_time from sending_buffer where sndbuff_snd_id=snd_id order by sndbuff_time desc) as inbufftime "
                             + " FROM sending left join sending_groups on sndg_snd_id=snd_id "
                             + " left join members on sndg_grp_id=mem_grp_id "
                             + " left join newsletter on snd_nl_id = nl_id "
                             + " where(snd_active = 1 And snd_sent = 0 And nl_active = 1 And mem_confirmed = 1)"
                             + " and (select count(sndbuff_id) from sending_buffer where sndbuff_snd_id=snd_id and sndbuff_mem_id=mem_id)=0 "
                             + " order by snd_id";

              if (ds.Tables.Contains("sending")) { ds.Tables["sending"].Clear(); }
              sqldata.Fill(ds, "sending");

              if (ds.Tables["sending"].Rows.Count > 0)
              {
                //leszedni a level body-t, es init local vals
                sqlcom.CommandText = "select snd_id2, snd_nl_id, snd_dateout, nl_subject, nl_id2, nl_cam_id, nl_unsubsc, nl_body, nl_textbody, user_sending, user_topsending, "
                         + " cam_email, cam_emailfromname, cam_sendmode, cam_doforwardpage, cam_unsubscrlnk, cam_doforward, cam_doforwardtxt, cam_unsubscrlnk, cam_unsubsc, cam_uniname "
                         + " from sending left join newsletter on snd_nl_id = nl_id left join campaigns on nl_cam_id=cam_id left join users on cam_user_id=user_id where snd_id=" + ds.Tables["sending"].Rows[0]["snd_id"].ToString();

                if (ds.Tables.Contains("body")) { ds.Tables["body"].Clear(); }
                sqldata.Fill(ds, "body");

                Int32 snd_id = Convert.ToInt32(ds.Tables["sending"].Rows[0]["snd_id"]);
                string nlbody = ds.Tables["body"].Rows[0]["nl_body"].ToString();
                string nltextbody = ds.Tables["body"].Rows[0]["nl_textbody"].ToString();
                Single incnum = 1000 / Convert.ToInt32(ds.Tables["body"].Rows[0]["user_sending"]);
                DateTime startdate;

                if (ds.Tables["sending"].Rows[0]["inbufftime"] == DBNull.Value)
                {
                  startdate = Convert.ToDateTime(ds.Tables["body"].Rows[0]["snd_dateout"]);
                }
                else
                {
                  startdate = Convert.ToDateTime(ds.Tables["sending"].Rows[0]["inbufftime"]);
                }

                string local = ds.Tables["settings"].Rows[0]["admin_localization"].ToString();

                Regex re = new Regex(Convert.ToChar(32) + "href=" + Convert.ToChar(34) + local + "/index_redirect.htm\\?id=\\{(\\w{8}-\\w{4}-\\w{4}-\\w{4}-\\w{12})");
                Regex tre = new Regex(local + "/index_redirect.htm\\?id=\\{(\\w{8}-\\w{4}-\\w{4}-\\w{4}-\\w{12})");

                //osszeszedjuk a cserelendo linkeket
                ArrayList links = new ArrayList();
                if (re.IsMatch(nlbody))
                {
                  foreach (Match tmp in re.Matches(nlbody))
                  {
                    if (!links.Contains(tmp.ToString()))
                    {
                      links.Add(tmp.ToString());
                    }
                  }
                }
                ArrayList linkstxt = new ArrayList();
                if (tre.IsMatch(nltextbody))
                {
                  foreach (Match tmp in tre.Matches(nltextbody))
                  {
                    if (!linkstxt.Contains(tmp.ToString()))
                    {
                      linkstxt.Add(tmp.ToString());
                    }
                  }
                }

                //vegigmegyunk es gyartjuk
                for (int i = 0; i < ds.Tables["sending"].Rows.Count; i++)
                {
                  nlbody = ds.Tables["body"].Rows[0]["nl_body"].ToString();
                  nltextbody = ds.Tables["body"].Rows[0]["nl_textbody"].ToString();

                  //ha uj feladat van reinit local vals
                  if (snd_id != Convert.ToInt32(ds.Tables["sending"].Rows[i]["snd_id"]))
                  {
                    try
                    {
                      sqlcom.CommandText = "update sending set snd_sent = 1 where snd_id=" + snd_id.ToString();
                      sqlcon.Open();
                      sqlcom.ExecuteNonQuery();
                      sqlcon.Close();
                    }
                    catch (Exception nextex)
                    {
                      ReportError(nextex);
                    }
                    snd_id = Convert.ToInt32(ds.Tables["sending"].Rows[0]["snd_id"]);

                    sqlcom.CommandText = "select snd_id2, snd_nl_id, snd_dateout, nl_subject, nl_id2, nl_cam_id, nl_unsubsc, nl_body, nl_textbody, user_sending, user_topsending, "
                     + " cam_email, cam_emailfromname, cam_sendmode, cam_doforwardpage, cam_unsubscrlnk, cam_doforward, cam_doforwardtxt, cam_unsubscrlnk, cam_unsubsc, cam_uniname "
                     + " from sending left join newsletter on snd_nl_id = nl_id left join campaigns on nl_cam_id=cam_id left join users on cam_user_id=user_id where snd_id=" + ds.Tables["sending"].Rows[i]["snd_id"].ToString();

                    if (ds.Tables.Contains("body")) { ds.Tables["body"].Clear(); }
                    sqldata.Fill(ds, "body");

                    incnum = 1000 / Convert.ToInt32(ds.Tables["body"].Rows[0]["user_sending"]);
                    nlbody = ds.Tables["body"].Rows[0]["nl_body"].ToString();
                    nltextbody = ds.Tables["body"].Rows[0]["nl_textbody"].ToString();
                  }
                  //sp proc creator
                  sqlcom.CommandText = " exec createsndbuffer2 @nl_id, @emailfrom, @emailfromname, @emailto, @subject, @mode, @time, @snd_id, @mem_id ";
                  sqlcom.Parameters.Clear();
                  sqlcom.Parameters.Add("@nl_id", SqlDbType.NVarChar, 100).Value = ds.Tables["body"].Rows[0]["snd_nl_id"];
                  sqlcom.Parameters.Add("@emailfrom", SqlDbType.NVarChar, 200).Value = ds.Tables["body"].Rows[0]["cam_email"];
                  sqlcom.Parameters.Add("@emailfromname", SqlDbType.NVarChar, 200).Value = ds.Tables["body"].Rows[0]["cam_emailfromname"];
                  sqlcom.Parameters.Add("@emailto", SqlDbType.NVarChar, 100).Value = ds.Tables["sending"].Rows[i]["mem_email"];
                  sqlcom.Parameters.Add("@subject", SqlDbType.NVarChar, 200).Value = ds.Tables["body"].Rows[0]["nl_subject"];
                  sqlcom.Parameters.Add("@mode", SqlDbType.NVarChar, 200).Value = ds.Tables["body"].Rows[0]["cam_sendmode"];
                  sqlcom.Parameters.Add("@snd_id", SqlDbType.NVarChar, 200).Value = ds.Tables["sending"].Rows[i]["snd_id"];
                  sqlcom.Parameters.Add("@mem_id", SqlDbType.NVarChar, 200).Value = ds.Tables["sending"].Rows[i]["mem_id"];

                  sqlcom.Parameters.Add("@time", SqlDbType.DateTime, 8).Value = startdate;
                  startdate = startdate.AddMilliseconds(incnum);

                  //kicsereljuk a linkeket a mert linkekre

                  foreach (string tmpitem in links)
                  {
                    nlbody = nlbody.Replace(tmpitem, tmpitem + "&mid={" + ds.Tables["sending"].Rows[i]["mem_id2"].ToString() + "&nlid={" + ds.Tables["body"].Rows[0]["nl_id2"].ToString() + "&sndid={" + ds.Tables["body"].Rows[0]["snd_id2"].ToString());
                  }

                  foreach (string tmpitem in linkstxt)
                  {
                    nltextbody = nltextbody.Replace(tmpitem, tmpitem + "&mid={" + ds.Tables["sending"].Rows[i]["mem_id2"].ToString() + "&nlid={" + ds.Tables["body"].Rows[0]["nl_id2"].ToString() + "&sndid={" + ds.Tables["body"].Rows[0]["snd_id2"].ToString());
                  }

                  //leiratkozas
                  if (Convert.ToBoolean(ds.Tables["body"].Rows[0]["cam_unsubscrlnk"]))
                  {
                    nlbody = nlbody.Replace("%leiratkozas%", "<a href=" + Convert.ToChar(34) + local + "/unsubscribe.htm?id={" + ds.Tables["sending"].Rows[i]["mem_id2"].ToString() + "&c=" + ds.Tables["body"].Rows[0]["nl_cam_id"].ToString() + Convert.ToChar(34) + ">" + ds.Tables["body"].Rows[0]["nl_unsubsc"].ToString() + "</a>");
                    nltextbody = nltextbody.Replace("%leiratkozas%", ds.Tables["body"].Rows[0]["nl_unsubsc"].ToString() + "(" + local + "/unsubscribe.htm?id={" + ds.Tables["sending"].Rows[i]["mem_id2"].ToString() + "&c=" + ds.Tables["body"].Rows[0]["nl_cam_id"].ToString() + ")");
                  }
                  //megnyitas stat
                  nlbody = nlbody + "<img src=" + Convert.ToChar(34) + local + "/images/spacer.gif?l={" + ds.Tables["body"].Rows[0]["nl_id2"].ToString() + "}&u={" + ds.Tables["sending"].Rows[i]["mem_id2"].ToString() + "}&s={" + ds.Tables["body"].Rows[0]["snd_id2"].ToString() + "}" + Convert.ToChar(34) + " width=" + Convert.ToChar(34) + "1" + Convert.ToChar(34) + " height=" + Convert.ToChar(34) + "1" + Convert.ToChar(34) + " nosend=" + Convert.ToChar(34) + "1" + Convert.ToChar(34) + ">";
                  //tovabbkuldes
                  if (Convert.ToBoolean(ds.Tables["body"].Rows[0]["cam_doforward"]))
                  {
                    string fwto = ds.Tables["body"].Rows[0]["cam_doforwardpage"].ToString();
                    if (fwto == "")
                    {
                      fwto = local + "/index_forward.htm";
                    }
                    if (fwto.Contains("?"))
                    {
                      fwto = fwto + "&";
                    }
                    else
                    {
                      fwto = fwto + "?";
                    }

                    nlbody = nlbody.Replace("%tovabb%", "<a href=" + Convert.ToChar(34) + fwto + "nlid={" + ds.Tables["body"].Rows[0]["nl_id2"].ToString() + "&mid={" + ds.Tables["sending"].Rows[i]["mem_id2"].ToString() + "&sndid={" + ds.Tables["body"].Rows[0]["snd_id2"].ToString() + Convert.ToChar(34) + ">" + ds.Tables["body"].Rows[0]["cam_doforwardtxt"].ToString() + "</a>");
                    nltextbody = nltextbody.Replace("%tovabb%", ds.Tables["body"].Rows[0]["cam_doforwardtxt"].ToString() + ": " + fwto + "nlid={" + ds.Tables["body"].Rows[0]["nl_id2"].ToString() + "&mid={" + ds.Tables["sending"].Rows[i]["mem_id2"].ToString() + "&sndid={" + ds.Tables["body"].Rows[0]["snd_id2"].ToString());
                  }
                  //letrehoz level
                  if (ds.Tables.Contains("sndbuff_id")) { ds.Tables["sndbuff_id"].Clear(); }
                  sqldata.Fill(ds, "sndbuff_id");


                  //gyartjuk a body-t
                  try
                  {
                    FileStream fs = new FileStream(ds.Tables["settings"].Rows[0]["admin_mailedtxtroot"].ToString() + "\\" + ds.Tables["sndbuff_id"].Rows[0]["sndbuff_id"].ToString() + ".txt", FileMode.Create, FileAccess.Write, FileShare.None);
                    FileStream fst = new FileStream(ds.Tables["settings"].Rows[0]["admin_mailedtxtroot"].ToString() + "\\" + ds.Tables["sndbuff_id"].Rows[0]["sndbuff_id"].ToString() + "_txt.txt", FileMode.Create, FileAccess.Write, FileShare.None);

                    StreamWriter s = new StreamWriter(fs, System.Text.Encoding.UTF8);
                    StreamWriter st = new StreamWriter(fst, System.Text.Encoding.UTF8);

                    s.BaseStream.Seek(0, SeekOrigin.End);
                    st.BaseStream.Seek(0, SeekOrigin.End);

                    string emname = "";

                    if (ds.Tables["body"].Rows[0]["cam_uniname"].ToString() != "")
                    {
                      emname = ds.Tables["body"].Rows[0]["cam_uniname"].ToString();
                    }

                    if (ds.Tables["sending"].Rows[i]["mem_name"].ToString() != "")
                    {
                      emname = ds.Tables["sending"].Rows[i]["mem_name"].ToString();
                    }


                    s.Write(nlbody.Replace("%nev%", emname));
                    st.Write(nltextbody.Replace("%nev%", emname));

                    s.Close();
                    st.Close();

                    s.Dispose();
                    st.Dispose();

                    fs.Dispose();
                    fst.Dispose();
                  }
                  catch (Exception fex)
                  {
                    //megnézzük e hogy van e legalább 500k szabad hely, ha nincs, várunk 30secet( c:, d:), hogy fogyjon a queue
                    ReportError(fex);
                    CheckFreeSpace(ds.Tables["settings"].Rows[0]["admin_mailedtxtroot"].ToString().Substring(0, 2));
                    i--;
                  }
                }

                sqlcom.CommandText = "SELECT top 1 snd_id FROM sending " +
                     "left join sending_groups on sndg_snd_id=snd_id inner join members on sndg_grp_id=mem_grp_id left " +
                     "join newsletter on snd_nl_id = nl_id left join campaigns on nl_cam_id=cam_id " +
                     "left join users on cam_user_id=user_id " +
                     "where(snd_active = 1 And snd_sent = 0 And nl_active = 1 and mem_confirmed=1) " +
                     "and mem_id not in (select sndbuff_mem_id from sending_buffer where sndbuff_snd_id=snd_id) order by snd_id ";

                if (ds.Tables.Contains("sents")) { ds.Tables["sents"].Clear(); }
                sqldata.Fill(ds, "sents");

                if (ds.Tables["sents"].Rows.Count == 0)
                {
                  try
                  {
                    sqlcom.CommandText = "update sending set snd_sent = 1 where snd_id=" + snd_id.ToString();
                    sqlcon.Open();
                    sqlcom.ExecuteNonQuery();
                    sqlcon.Close();
                  }
                  catch (Exception endex)
                  {
                    ReportError(endex);
                  }
                }
              }
            }
            catch (Exception sendex)
            {
              ReportError(sendex);
            }
          }
          sqlcom.Dispose();
          sqlcon.Dispose();
          ds.Dispose();
        }
      }
      catch (Exception cex)
      {
        ReportError(cex);
      }
      finally
      {
        GC.Collect();
        GC.WaitForPendingFinalizers();
        GC.Collect();
      }
    }

    private void CheckFreeSpace(string drive)
    {
      bool sleep_req = false;
      do
      {
        if (sleep_req)
        {
          System.Threading.Thread.Sleep(15 * 1000);
          sleep_req = false;
        }
        try
        {
          ManagementObject disk = new ManagementObject("win32_logicaldisk.deviceid=\"" + drive + "\"");
          disk.Get();
          if (Convert.ToInt64(disk["FreeSpace"]) < 5242880)
          {
            SendMsg(disk["FreeSpace"].ToString());
            throw new ManagementException();
          }
        }
        catch (ManagementException mex)
        {
          EventLog.WriteEntry("Out of disk space error: ", "CCreator Creator: " + mex.ToString(), EventLogEntryType.Warning);
          EventLog.WriteEntry("Sleeping for 15 seconds...", "CCreator Creator");
          sleep_req = true;
        }
      } while (sleep_req);
    }

    private void ReportError(Exception e_)
    {
      EventLog.WriteEntry(string.Format("Error in {0}: {1}", ClassName, e_.ToString()), EventLogEntryType.Error);
    }

    private void SendMsg(Exception e_)
    {
      EventLog.WriteEntry(string.Format("Error in {0}: {1}", ClassName, e_.ToString()), EventLogEntryType.Error);
    }

    private void SendMsg(string message_)
    {
      EventLog.WriteEntry(string.Format("Message from {0}: {1}", ClassName, message_), EventLogEntryType.Warning);
    }
  }
}
