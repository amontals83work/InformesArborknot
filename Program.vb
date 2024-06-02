Imports System
Imports System.Data
Imports System.Data.SqlClient
Imports System.IO
Imports System.Text.RegularExpressions
Imports System.Globalization
Imports System.Runtime.InteropServices.JavaScript.JSType
Imports System.Security.Cryptography.X509Certificates
Imports System.Diagnostics.Metrics

Module Program
    Function formatPagado(pagado As String) As String
        Dim puntoPos As Integer
        puntoPos = InStr(pagado, ".")
        If puntoPos > 0 And Len(pagado) > puntoPos + 2 Then
            formatPagado = Left(pagado, puntoPos + 2)
        Else
            formatPagado = pagado
        End If
    End Function
    Function tipoNota(nota As Integer) As String
        Select Case nota
            Case 5 To 11
                Return "Letter"
            Case 13, 14, 15, 789
                Return "Fax"
            Case 22, 742
                Return "Phone"
            Case 79 To 81
                Return "Letter"
            Case 656, 744, 746, 854
                Return "Letter"
            Case 743
                Return "Others"
            Case 788, 840
                Return "SMS"
            Case 820, 821, 822, 823, 824, 825, 826, 827, 828, 831, 832, 834, 836, 837, 849, 861, 917, 918, 921, 922
                Return "Email"
            Case Else
                Return "Others"
        End Select
    End Function
    Function tipoLlamada1(nota As Integer) As String
        Select Case nota
            Case 1, 2, 5, 6, 7
                Return "Outbound"
            Case 3, 4, 8, 9, 10, 13, 14, 15
                Return "Inbound"
            Case Else
                Return "Undefined"
        End Select
    End Function
    Function tipoLlamada2(nota As Integer) As String
        Select Case nota
            Case 5, 6, 7, 8, 9, 10, 79, 80, 81, 656, 744, 746, 788, 789, 820, 821, 822, 823, 824, 825, 826, 827, 828, 831, 832, 834, 836, 837, 840, 849, 854, 917, 918, 921, 922
                Return "Outbound"
            Case 11, 13, 14, 15, 861
                Return "Inbound"
            Case Else
                Return "Undefined"
        End Select
    End Function
    Function resultadoAccion(resultado As Integer) As String
        Select Case resultado
            Case 4
                Return "Hardship"
            Case 8
                Return "Deceased"
            Case 9
                Return "Bankruptcy"
            Case 13, 15
                Return "Cease and Desist"
            Case Else
                Return "Active"
        End Select
    End Function
    Function contador(count As Integer) As String
        If count Mod 100000 = 0 Then
            Console.WriteLine(count.ToString + " ")
        ElseIf count Mod 5000 = 0 Then
            Console.Write(count.ToString + " ")
        End If
    End Function

    Sub Main(args As String())
        Dim idExpediente As String, count As Integer = 0
        Dim internatDebtId As Object, bookId As Object, originalAcountId As Object, fecha As Object, formatFecha As String, pagado As Object, anyo As String, mes As String, dia As String, hora As String
        Dim idTipoNota As Object, idResultado As Object, debtStatusID As String, observaciones As String
        Dim messageId As Object, idTipoAccion As Object, channelId As String
        Dim cmdRecibos As New SqlCommand
        Dim stopwatch As New Stopwatch()
        Dim ts As TimeSpan
        Dim elapsedTime As String = ""
        Dim readerAux1 As SqlDataReader, readerAux2 As SqlDataReader, readerAux3 As SqlDataReader, readerAux4 As SqlDataReader
        stopwatch.Start()

        Dim path As String = "\\192.168.50.46\e\Alfonso\Informes Diarios Arborknot" '\Historicos" 'Environment.GetFolderPath(Environment.SpecialFolder.MyDocuments)
        'Dim today As String = Date.Today.ToString("yyyyMMdd")
        Dim today As String = "Crisalida"
        If Not Directory.Exists(path & "\" & today) Then
            Directory.CreateDirectory(path & "\" & today)
            path = path & "\" & today
        Else
            path = path & "\" & today
        End If

        Dim connection As New SqlConnection("Data Source=192.168.50.48;Initial Catalog=DespachoMc;Persist Security Info=True;User ID=sa;Password=Binabiq2018_;MultipleActiveResultSets=True;")
        connection.Open()

        ' COLLECTION /////////////////////////////////////////////////////////////////////////////////////////////////////////////////

        'Dim cmdIncidencia As New SqlCommand("SELECT I.IdIncidencia, I.IdExpediente, I.Pagado, I.Fecha FROM Incidencias I INNER JOIN Expedientes E ON I.IdExpediente = E.IdExpediente WHERE Fecha >= CAST(GETDATE() AS DATE) AND E.idCliente IN (84, 85, 86, 87, 88, 89, 90, 91) ORDER BY Fecha ASC", connection)
        'Dim cmdIncidencia As New SqlCommand("SELECT I.IdIncidencia, I.IdExpediente, I.Pagado, I.Fecha FROM Incidencias I INNER JOIN Expedientes E ON I.IdExpediente = E.IdExpediente WHERE Fecha >= CONVERT(DATETIME, '2024-04-18 00:00:00', 120) AND Fecha < CONVERT(DATETIME, '2024-04-19 00:00:00', 120) AND E.idCliente IN (84, 85, 86, 87, 88, 89, 90, 91) ORDER BY Fecha ASC", connection)
        Dim cmdIncidencia As New SqlCommand("SELECT I.IdIncidencia, I.IdExpediente, I.Pagado, I.Fecha FROM Incidencias I INNER JOIN Expedientes E ON I.IdExpediente = E.IdExpediente WHERE CONVERT(DATE,Fecha) >= '26/07/2023' AND CONVERT(DATE,Fecha) < '31/05/2024' AND E.idCliente=87 ORDER BY Fecha ASC", connection)
        Dim rIncidencias As SqlDataReader = cmdIncidencia.ExecuteReader()
        Dim writerColl As New StreamWriter(IO.Path.Combine(path, "Daily_collection_report_MC2Legal_" & today & ".csv"))
        writerColl.WriteLine("Payment ID,InternalBookID,InternalDebtID,OriginalAccountID,Timestamp,Amount")
        While rIncidencias.Read()
            idExpediente = rIncidencias("IdExpediente")
            cmdRecibos = New SqlCommand("SELECT NumFactura, codigobarras, url FROM Recibos WHERE IdExpediente = @idExpediente", connection)
            cmdRecibos.Parameters.Clear()
            cmdRecibos.Parameters.Add("@idExpediente", SqlDbType.Int).Value = idExpediente
            readerAux1 = cmdRecibos.ExecuteReader()

            If readerAux1.Read Then
                originalAcountId = If(Not readerAux1.IsDBNull("NumFactura"), readerAux1.GetString("NumFactura").Replace(",", "."), "")
                bookId = If(Not readerAux1.IsDBNull("codigobarras"), readerAux1.GetString("codigobarras"), "")
                internatDebtId = If(Not readerAux1.IsDBNull("url"), readerAux1.GetString("url").Replace(",", "."), "")
            End If
            readerAux1.Close()

            fecha = If(rIncidencias("Fecha") Is Nothing, "", rIncidencias("Fecha").ToString.Replace("/", "-"))
            anyo = Mid(fecha, 7, 4)
            mes = Mid(fecha, 4, 2)
            dia = Mid(fecha, 1, 2)
            hora = Mid(fecha, 12, 8)
            formatFecha = anyo & "-" & mes & "-" & dia & " " & hora
            pagado = If(rIncidencias("Pagado") Is Nothing, "", formatPagado(rIncidencias("Pagado").ToString.Replace(",", ".")))

            count += 1
            contador(count)

            writerColl.WriteLine(rIncidencias("IdIncidencia").ToString & "," & bookId.ToString & "," & internatDebtId.ToString & "," & originalAcountId.ToString & "," & formatFecha & "," & pagado)
        End While
        rIncidencias.Close()
        writerColl.Close()

        ts = stopwatch.Elapsed
        elapsedTime = String.Format("{0:00}:{1:00}:{2:00}", ts.Hours, ts.Minutes, ts.Seconds)
        Console.WriteLine("COLLECTION CREADO. Tiempo: " & elapsedTime.ToString + vbCrLf)

        '' STATUS /////////////////////////////////////////////////////////////////////////////////////////////////////////////////

        'Dim cmdAcciones1 As New SqlCommand("SELECT A.IdAccion, A.IdExpediente, A.idTipoNota FROM Acciones A JOIN Expedientes E ON A.IdExpediente = E.IdExpediente WHERE A.Fecha >= CAST(GETDATE() AS DATE) AND E.idCliente in (84,85,86,87,88,89,90,91) ORDER BY A.Fecha ASC", connection)
        'Dim cmdAcciones1 As New SqlCommand("SELECT A.IdAccion, A.IdExpediente, A.idTipoNota FROM Acciones A JOIN Expedientes E ON A.IdExpediente = E.IdExpediente WHERE A.Fecha >= CONVERT(DATETIME, '2024-04-18 00:00:00', 120) AND Fecha < CONVERT(DATETIME, '2024-04-19 00:00:00', 120) AND E.idCliente in (84,85,86,87,88,89,90,91) ORDER BY A.Fecha ASC", connection)
        Dim cmdAcciones1 As New SqlCommand("SELECT A.IdAccion, A.IdExpediente, A.idTipoNota, A.Observaciones FROM Acciones A JOIN Expedientes E ON A.IdExpediente = E.IdExpediente WHERE CONVERT(DATE,A.Fecha) >= '26/07/2023' AND CONVERT(DATE,A.Fecha) < '31/05/2024' AND E.idCliente=87 ORDER BY A.Fecha ASC", connection)
        Dim rAcciones1 As SqlDataReader = cmdAcciones1.ExecuteReader()
        Dim writerStat As New StreamWriter(IO.Path.Combine(path, "Daily_account_status_report_MC2Legal_" & today & ".csv"))
        writerStat.WriteLine("InternalBookID,InternalDebtID,OriginalAccountID,DebtStatusID")
        Dim cmdTipoAccion As New SqlCommand, cmdTipoNota1 As New SqlCommand
        count = 0
        While rAcciones1.Read()
            cmdRecibos = New SqlCommand("SELECT NumFactura, codigobarras, url FROM Recibos WHERE IdExpediente = @idExpediente", connection)
            cmdRecibos.Parameters.Clear()
            cmdRecibos.Parameters.Add("@idExpediente", SqlDbType.Int).Value = rAcciones1("IdExpediente")
            readerAux2 = cmdRecibos.ExecuteReader()

            If readerAux2.Read Then
                originalAcountId = If(Not readerAux2.IsDBNull("NumFactura"), readerAux2.GetString("NumFactura").Replace(",", "."), "")
                bookId = If(Not readerAux2.IsDBNull("codigobarras"), readerAux2.GetString("codigobarras"), "")
                internatDebtId = If(Not readerAux2.IsDBNull("url"), readerAux2.GetString("url").Replace(",", "."), "")
            End If
            readerAux2.Close()

            idTipoNota = If(rAcciones1("idTipoNota") Is Nothing, "", Convert.ToUInt32(rAcciones1("idTipoNota")))
            observaciones = If(String.IsNullOrEmpty(rAcciones1("Observaciones").ToString()), "", rAcciones1("Observaciones").ToString())
            If idTipoNota = 17 Then
                debtStatusID = If(observaciones.Contains("PAGADO"), "Paid", "Litigious")
            ElseIf idTipoNota = 802 Then
                debtStatusID = "Active"
            Else
                cmdTipoAccion = New SqlCommand("SELECT IdResultado FROM ResultadoLlamada WHERE IdAccion = @idAccion", connection)
                cmdTipoAccion.Parameters.Clear()
                cmdTipoAccion.Parameters.Add("@idAccion", SqlDbType.Int).Value = rAcciones1("IdAccion")
                readerAux3 = cmdTipoAccion.ExecuteReader()

                If readerAux3.Read Then
                    idResultado = If(Not readerAux3.IsDBNull(readerAux3.GetOrdinal("IdResultado")) AndAlso Not String.IsNullOrEmpty(readerAux3("IdResultado").ToString()), CInt(readerAux3("IdResultado")), 0)
                    debtStatusID = resultadoAccion(CInt(idResultado))
                Else
                    debtStatusID = "Active"
                End If
                readerAux3.Close()
            End If

            count += 1
            contador(count)

            writerStat.WriteLine(bookId.ToString & "," & internatDebtId.ToString & "," & originalAcountId.ToString & "," & debtStatusID.ToString)
        End While
        rAcciones1.Close()
        writerStat.Close()

        ts = stopwatch.Elapsed
        elapsedTime = String.Format("{0:00}:{1:00}:{2:00}", ts.Hours, ts.Minutes, ts.Seconds)
        Console.WriteLine(vbCrLf + "STATUS CREADO. Tiempo: " & elapsedTime.ToString + vbCrLf)

        ' COMMUNICATION /////////////////////////////////////////////////////////////////////////////////////////////////////////////////

        'Dim cmdAcciones2 As New SqlCommand("SELECT A.IdAccion, A.IdExpediente, TN.idTipoNota, A.Fecha, RL.IdTipoAccion FROM Acciones AS A JOIN Expedientes AS E ON A.IdExpediente = E.IdExpediente LEFT JOIN ResultadoLlamada AS RL ON A.IdAccion = RL.IdAccion LEFT JOIN TipoNota AS TN ON A.IdTipoNota = TN.idTipoNota WHERE A.Fecha >= CAST(GETDATE() AS DATE) AND E.idCliente IN (84, 85, 86, 87, 88, 89, 90, 91) AND A.idTipoNota IN (5, 7, 8, 9, 10, 11, 13, 14, 15, 22, 79, 80, 81, 656, 742, 744, 746, 788, 789, 820, 821, 822, 823, 824, 825, 826, 827, 828, 831, 832, 834, 836, 837, 840, 849, 854, 861, 915, 917, 918, 921, 922) ORDER BY A.Fecha ASC", connection)
        'Dim cmdAcciones2 As New SqlCommand("SELECT A.IdAccion, A.IdExpediente, TN.idTipoNota, A.Fecha, RL.IdTipoAccion FROM Acciones AS A JOIN Expedientes AS E ON A.IdExpediente = E.IdExpediente LEFT JOIN ResultadoLlamada AS RL ON A.IdAccion = RL.IdAccion LEFT JOIN TipoNota AS TN ON A.IdTipoNota = TN.idTipoNota WHERE A.Fecha >= CONVERT(DATETIME, '2024-04-18 00:00:00', 120) AND A.Fecha < CONVERT(DATETIME, '2024-04-19 00:00:00', 120) AND E.idCliente IN (84, 85, 86, 87, 88, 89, 90, 91) AND A.idTipoNota IN (5, 7, 8, 9, 10, 11, 13, 14, 15, 22, 79, 80, 81, 656, 742, 744, 746, 788, 789, 820, 821, 822, 823, 824, 825, 826, 827, 828, 831, 832, 834, 836, 837, 840, 849, 854, 861, 915, 917, 918, 921, 922) ORDER BY A.Fecha ASC", connection)
        Dim cmdAcciones2 As New SqlCommand("SELECT A.IdAccion, A.IdExpediente, TN.idTipoNota, A.Fecha, RL.IdTipoAccion FROM Acciones AS A JOIN Expedientes AS E ON A.IdExpediente = E.IdExpediente LEFT JOIN ResultadoLlamada AS RL ON A.IdAccion = RL.IdAccion LEFT JOIN TipoNota AS TN ON A.IdTipoNota = TN.idTipoNota WHERE CONVERT(DATE,A.Fecha) >= '26/07/2023' AND CONVERT(DATE,A.Fecha) < '31/05/2024' AND E.idCliente=87 AND A.idTipoNota IN (5, 7, 8, 9, 10, 11, 13, 14, 15, 22, 79, 80, 81, 656, 742, 744, 746, 788, 789, 820, 821, 822, 823, 824, 825, 826, 827, 828, 831, 832, 834, 836, 837, 840, 849, 854, 861, 915, 917, 918, 921, 922) ORDER BY A.Fecha ASC", connection)
        Dim rAcciones2 As SqlDataReader = cmdAcciones2.ExecuteReader()
        Dim writerComm As New StreamWriter(IO.Path.Combine(path, "Daily_communication_report_MC2Legal_" & today & ".csv"))
        writerComm.WriteLine("ExternalProviderMessageID,OriginalAccountID,InternalDebtID,ChannelID,ExecutedAt,Direction")
        count = 0
        While rAcciones2.Read()
            messageId = rAcciones2("IdAccion")

            cmdRecibos = New SqlCommand("SELECT NumFactura, url FROM Recibos WHERE IdExpediente = @idExpediente", connection)
            cmdRecibos.Parameters.Clear()
            cmdRecibos.Parameters.Add("@idExpediente", SqlDbType.Int).Value = rAcciones2("IdExpediente")
            readerAux4 = cmdRecibos.ExecuteReader()

            If readerAux4.Read Then
                originalAcountId = If(Not readerAux4.IsDBNull("NumFactura"), readerAux4.GetString("NumFactura").Replace(",", "."), "")
                internatDebtId = If(Not readerAux4.IsDBNull("url"), readerAux4.GetString("url").Replace(",", "."), "")
            End If
            readerAux4.Close()

            idTipoNota = If(rAcciones2("idTipoNota") Is Nothing, "", Convert.ToUInt32(rAcciones2("idTipoNota")))

            channelId = tipoNota(idTipoNota)

            fecha = If(rAcciones2("Fecha") Is Nothing, "", rAcciones2("Fecha").ToString.Replace("/", "-"))
            anyo = Mid(fecha, 7, 4)
            mes = Mid(fecha, 4, 2)
            dia = Mid(fecha, 1, 2)
            hora = Mid(fecha, 12, 8)
            formatFecha = anyo & "-" & mes & "-" & dia & " " & hora

            If rAcciones2("IdTipoAccion") Is Nothing Or rAcciones2("IdTipoAccion") Is DBNull.Value Then
                idTipoAccion = tipoLlamada2(CInt(idTipoNota))
            Else
                idTipoAccion = tipoLlamada1(CInt(rAcciones2("IdTipoAccion")))
            End If

            count += 1
            contador(count)

            writerComm.WriteLine(messageId.ToString & "," & originalAcountId.ToString & "," & internatDebtId.ToString & "," & channelId & "," & formatFecha & "," & idTipoAccion.ToString)
        End While
        rAcciones2.Close()
        writerComm.Close()

        ts = stopwatch.Elapsed
        elapsedTime = String.Format("{0:00}:{1:00}:{2:00}", ts.Hours, ts.Minutes, ts.Seconds)
        Console.WriteLine(vbCrLf + "COMMUNICATION CREADO. Tiempo: " & elapsedTime)

        ' /////////////////////////////////////////////////////////////////////////////////////////////////////////////////

        connection.Close()
        stopwatch.Stop()
    End Sub

    'Sub Main(args As String())
    '    Dim stopwatch As New Stopwatch()
    '    Dim count As Integer, countT As Integer = 0
    '    stopwatch.Start()

    '    Dim connection As New SqlConnection("Data Source=192.168.50.48;Initial Catalog=DespachoMc;Persist Security Info=True;User ID=sa;Password=Binabiq2018_;MultipleActiveResultSets=True;")
    '    'Dim today As String = Date.Today.ToString("yyyyMMdd")
    '    Dim today As String = "20240405_"
    '    Dim path As String = "\\192.168.50.46\e\Alfonso\Informes Diarios Arborknot" 'Environment.GetFolderPath(Environment.SpecialFolder.MyDocuments)
    '    If Not Directory.Exists(path & "\" & today) Then
    '        Directory.CreateDirectory(path & "\" & today)
    '    End If
    '    path = path & "\" & today

    '    connection.Open()

    '    ' COLLECTION /////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    '    'Dim cmdIncidencia As New SqlCommand("SELECT I.IdIncidencia, I.IdExpediente, I.Pagado, I.Fecha FROM Incidencias I INNER JOIN Expedientes E ON I.IdExpediente = E.IdExpediente WHERE Fecha >= CAST(GETDATE() AS DATE) AND E.idCliente IN (84, 85, 86, 87, 88, 89, 90, 91) ORDER BY Fecha ASC", connection)
    '    Dim cmdIncidencia As New SqlCommand("SELECT I.IdIncidencia, I.IdExpediente, I.Pagado, I.Fecha FROM Incidencias I INNER JOIN Expedientes E ON I.IdExpediente = E.IdExpediente WHERE Fecha >= CONVERT(DATETIME, '2024-04-05 00:00:00', 120) AND Fecha < CONVERT(DATETIME, '2024-04-06 00:00:00', 120) AND E.idCliente IN (84, 85, 86, 87, 88, 89, 90, 91) ORDER BY Fecha ASC", connection)
    '    Dim rIncidencias As SqlDataReader = cmdIncidencia.ExecuteReader()
    '    Dim idExpediente As String, codFactura As Object, codigoBarras As Object, fecha As Object, formatFecha As String, pagado As Object, anyo As String, mes As String, dia As String, hora As String

    '    Dim writerColl As New StreamWriter(IO.Path.Combine(path, "Daily_collection_report_MC2Legal_" & today & ".csv"))
    '    writerColl.WriteLine("Payment ID,InternalDebtID,OriginalAccountID,Timestamp,Amount")
    '    Dim cmdRecibos As New SqlCommand
    '    While rIncidencias.Read()
    '        idExpediente = rIncidencias("IdExpediente")

    '        cmdRecibos = New SqlCommand("SELECT CodFactura, codigobarras FROM Recibos WHERE IdExpediente = @idExpediente", connection)
    '        cmdRecibos.Parameters.Add("@idExpediente", SqlDbType.Int).Value = idExpediente

    '        Dim reader As SqlDataReader = cmdRecibos.ExecuteReader()
    '        If reader.Read Then
    '            codFactura = reader.GetString(0).Replace(",", "")
    '            If Not reader.IsDBNull(1) Then
    '                codigoBarras = reader.GetString(1).Replace(",", "")
    '            Else
    '                codigoBarras = ""
    '            End If
    '        Else
    '            codFactura = ""
    '            codigoBarras = ""
    '        End If

    '        reader.Close()

    '        fecha = If(rIncidencias("Fecha") Is Nothing, "", rIncidencias("Fecha").ToString.Replace("/", "-"))
    '        anyo = Mid(fecha, 7, 4)
    '        mes = Mid(fecha, 4, 2)
    '        dia = Mid(fecha, 1, 2)
    '        hora = Mid(fecha, 12, 8)

    '        formatFecha = anyo & "-" & mes & "-" & dia & " " & hora
    '        pagado = If(rIncidencias("Pagado") Is Nothing, "", formatPagado(rIncidencias("Pagado").ToString.Replace(",", ".")))

    '        count += 1
    '        If count = 1000 Then
    '            If countT = 10000 Then
    '                countT = countT + count
    '                count = 0
    '                Console.WriteLine(countT.ToString + " ")
    '            ElseIf countT = 20000 Then
    '                countT = countT + count
    '                count = 0
    '                Console.WriteLine(countT.ToString + " ")
    '            ElseIf countT = 30000 Then
    '                countT = countT + count
    '                count = 0
    '                Console.WriteLine(countT.ToString + " ")
    '            Else
    '                countT = countT + count
    '                count = 0
    '                Console.Write(countT.ToString + " ")
    '            End If
    '        End If

    '        writerColl.WriteLine(rIncidencias("IdIncidencia").ToString.Trim & "," & codigoBarras.ToString.Trim & "," & codFactura.ToString.Trim & "," & formatFecha & "," & pagado)
    '    End While
    '    rIncidencias.Close()
    '    writerColl.Close()
    '    Console.WriteLine("CSV 1 Collestions creado. Tiempo: " & stopwatch.Elapsed.ToString)

    '    ' STATUS /////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    '    'Dim cmdAcciones1 As New SqlCommand("SELECT A.IdAccion, A.IdExpediente FROM Acciones A JOIN Expedientes E ON A.IdExpediente = E.IdExpediente WHERE A.Fecha >= CAST(GETDATE() AS DATE) AND E.idCliente in (84,85,86,87,88,89,90,91) ORDER BY A.Fecha ASC", connection) 
    '    Dim cmdAcciones1 As New SqlCommand("SELECT A.IdAccion, A.IdExpediente FROM Acciones A JOIN Expedientes E ON A.IdExpediente = E.IdExpediente WHERE A.Fecha >= CONVERT(DATETIME, '2024-04-05 00:00:00', 120) AND Fecha < CONVERT(DATETIME, '2024-04-06 00:00:00', 120) AND E.idCliente in (84,85,86,87,88,89,90,91) ORDER BY A.Fecha ASC", connection)
    '    Dim rAcciones1 As SqlDataReader = cmdAcciones1.ExecuteReader()
    '    Dim idAccion As Object, descripcionNota As Object

    '    Dim writerStat As New StreamWriter(IO.Path.Combine(path, "Daily_account_status_report_MC2Legal_" & today & ".csv"))
    '    writerStat.WriteLine("InternalDebtID,OriginalAccountID,Account status")
    '    Dim cmdTipoNota1 As New SqlCommand
    '    While rAcciones1.Read()
    '        idExpediente = rAcciones1("IdExpediente")

    '        cmdRecibos = New SqlCommand("SELECT CodFactura, codigobarras FROM Recibos WHERE IdExpediente = @idExpediente", connection)
    '        cmdRecibos.Parameters.Add("@idExpediente", SqlDbType.Int).Value = idExpediente

    '        Dim reader As SqlDataReader = cmdRecibos.ExecuteReader()
    '        If reader.Read Then
    '            codFactura = reader.GetString(0).Replace(",", "")
    '            If Not reader.IsDBNull(1) Then
    '                codigoBarras = reader.GetString(1).Replace(",", "")
    '            Else
    '                codigoBarras = ""
    '            End If
    '        Else
    '            codFactura = ""
    '            codigoBarras = ""
    '        End If

    '        reader.Close()

    '        idAccion = rAcciones1("IdAccion")
    '        cmdTipoNota1 = New SqlCommand("SELECT Descripcion FROM ( SELECT t.Descripcion, a.IdAccion FROM TipoNota AS t INNER JOIN Acciones AS a ON t.idTipoNota = a.idTipoNota WHERE a.IdAccion=" & idAccion & ") AS T GROUP BY Descripcion", connection)
    '        descripcionNota = If(cmdTipoNota1.ExecuteScalar() Is Nothing, "", cmdTipoNota1.ExecuteScalar().ToString().Replace(",", ""))
    '        count += 1
    '        Console.Write(vbCrLf)
    '        If count = 1000 Then
    '            If countT = 10000 Then
    '                countT = countT + count
    '                count = 0
    '                Console.Write(vbCrLf + countT.ToString + " ")
    '            ElseIf countT = 20000 Then
    '                countT = countT + count
    '                count = 0
    '                Console.Write(vbCrLf + countT.ToString + " ")
    '            ElseIf countT = 30000 Then
    '                countT = countT + count
    '                count = 0
    '                Console.Write(vbCrLf + countT.ToString + " ")
    '            Else
    '                countT = countT + count
    '                count = 0
    '                Console.Write(countT.ToString + " ")
    '            End If
    '        End If

    '        writerStat.WriteLine(codigoBarras.ToString & "," & codFactura.ToString & "," & descripcionNota.ToString)
    '    End While
    '    rAcciones1.Close()
    '    writerStat.Close()
    '    Console.WriteLine(vbCrLf + "CSV 2 Statuses creado. Tiempo: " & stopwatch.Elapsed.ToString)

    '    ' COMMUNICATION /////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    '    'Dim cmdAcciones2 As New SqlCommand("SELECT A.IdExpediente, TN.idTipoNota, A.Fecha, RL.IdTipoAccion FROM Acciones AS A JOIN Expedientes AS E ON A.IdExpediente = E.IdExpediente LEFT JOIN ResultadoLlamada AS RL ON A.IdAccion = RL.IdAccion LEFT JOIN TipoNota AS TN ON A.IdTipoNota = TN.idTipoNota WHERE A.Fecha >= CAST(GETDATE() AS DATE) AND E.idCliente IN (84, 85, 86, 87, 88, 89, 90, 91) AND A.idTipoNota IN (5, 7, 8, 9, 10, 11, 13, 14, 15, 22, 79, 80, 81, 656, 742, 743, 744, 746, 788, 789, 820, 821, 822, 823, 824, 825, 826, 827, 828, 831, 832, 834, 836, 837, 840, 849, 854, 861, 915, 917, 918, 921, 922) ORDER BY A.Fecha ASC", connection)
    '    Dim cmdAcciones2 As New SqlCommand("SELECT A.IdExpediente, TN.idTipoNota, A.Fecha, RL.IdTipoAccion FROM Acciones AS A JOIN Expedientes AS E ON A.IdExpediente = E.IdExpediente LEFT JOIN ResultadoLlamada AS RL ON A.IdAccion = RL.IdAccion LEFT JOIN TipoNota AS TN ON A.IdTipoNota = TN.idTipoNota WHERE A.Fecha >= CONVERT(DATETIME, '2024-04-05 00:00:00', 120) AND A.Fecha < CONVERT(DATETIME, '2024-04-06 00:00:00', 120) AND E.idCliente IN (84, 85, 86, 87, 88, 89, 90, 91) AND A.idTipoNota IN (5, 7, 8, 9, 10, 11, 13, 14, 15, 22, 79, 80, 81, 656, 742, 743, 744, 746, 788, 789, 820, 821, 822, 823, 824, 825, 826, 827, 828, 831, 832, 834, 836, 837, 840, 849, 854, 861, 915, 917, 918, 921, 922) ORDER BY A.Fecha ASC", connection)
    '    Dim rAcciones2 As SqlDataReader = cmdAcciones2.ExecuteReader()
    '    Dim numTipoNota As Object, idTipoAccion As Object, descripcion As String

    '    Dim writerComm As New StreamWriter(IO.Path.Combine(path, "Daily_communication_report_MC2Legal_" & today & ".csv"))
    '    writerComm.WriteLine("ExternalProviderMessageID,OriginalAccountID,InternalDebtID,ChannelID,ExecutedAt,Direction")
    '    Dim cmdTipoNota2 As New SqlCommand

    '    fecha = ""
    '    formatFecha = ""
    '    count = 0
    '    While rAcciones2.Read()
    '        idExpediente = rAcciones2("IdExpediente")
    '        numTipoNota = If(rAcciones2("idTipoNota") Is Nothing, "", Convert.ToInt32(rAcciones2("idTipoNota")))
    '        fecha = If(rAcciones2("Fecha") Is Nothing, "", rAcciones2("Fecha").ToString.Replace("/", "-"))
    '        anyo = Mid(fecha, 7, 4)
    '        mes = Mid(fecha, 4, 2)
    '        dia = Mid(fecha, 1, 2)
    '        hora = Mid(fecha, 12, 8)
    '        formatFecha = anyo & "-" & mes & "-" & dia & " " & hora

    '        descripcion = tipoNota(numTipoNota)

    '        If rAcciones2("IdTipoAccion") Is Nothing Or rAcciones2("IdTipoAccion") Is DBNull.Value Then
    '            idTipoAccion = tipoContacto(numTipoNota)
    '        Else
    '            idTipoAccion = tipoLlamada(Convert.ToUInt32(rAcciones2("IdTipoAccion")))
    '        End If

    '        cmdRecibos = New SqlCommand("SELECT CodFactura, codigobarras FROM Recibos WHERE IdExpediente = @idExpediente", connection)
    '        cmdRecibos.Parameters.Add("@idExpediente", SqlDbType.Int).Value = idExpediente

    '        Dim reader As SqlDataReader = cmdRecibos.ExecuteReader()
    '        If reader.Read Then
    '            codFactura = reader.GetString(0).Replace(",", "")
    '            If Not reader.IsDBNull(1) Then
    '                codigoBarras = reader.GetString(1).Replace(",", "")
    '            Else
    '                codigoBarras = ""
    '            End If
    '        Else
    '            codFactura = ""
    '            codigoBarras = ""
    '        End If

    '        reader.Close()
    '        count += 1
    '        Console.Write(vbCrLf)
    '        If count = 1000 Then
    '            If countT = 10000 Then
    '                countT = countT + count
    '                count = 0
    '                Console.Write(vbCrLf + countT.ToString + " ")
    '            ElseIf countT = 20000 Then
    '                countT = countT + count
    '                count = 0
    '                Console.Write(vbCrLf + countT.ToString + " ")
    '            ElseIf countT = 30000 Then
    '                countT = countT + count
    '                count = 0
    '                Console.Write(vbCrLf + countT.ToString + " ")
    '            Else
    '                countT = countT + count
    '                count = 0
    '                Console.Write(countT.ToString + " ")
    '            End If
    '        End If

    '        writerComm.WriteLine("MC2 Legal," & codFactura.ToString & "," & codigoBarras.ToString & "," & descripcion & "," & formatFecha & "," & idTipoAccion.ToString)
    '    End While
    '    rAcciones2.Close()
    '    writerComm.Close()
    '    Console.WriteLine(vbCrLf + "CSV 3 Communications creado. Tiempo: " & stopwatch.Elapsed.ToString)

    '    connection.Close()
    '    stopwatch.Stop()
    'End Sub

End Module
