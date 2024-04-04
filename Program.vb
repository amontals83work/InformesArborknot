Imports System
Imports System.Data
Imports System.Data.SqlClient
Imports System.IO
Imports System.Text.RegularExpressions
Imports System.Globalization
Imports System.Runtime.InteropServices.JavaScript.JSType


Module Program
    Sub Main(args As String())
        Dim stopwatch As New Stopwatch()
        stopwatch.Start()

        Dim connection As New SqlConnection("Data Source=192.168.50.48;Initial Catalog=DespachoMc;Persist Security Info=True;User ID=sa;Password=Binabiq2018_;MultipleActiveResultSets=True;")
        Dim today As String = Date.Today.ToString("yyyyMMdd")
        'Dim today As String = "20240403"
        Dim path As String = "\\192.168.50.46\e\Alfonso\Informes Diarios Arborknot" 'Environment.GetFolderPath(Environment.SpecialFolder.MyDocuments)
        If Not Directory.Exists(path & "\" & today) Then
            Directory.CreateDirectory(path & "\" & today)
        End If
        path = path & "\" & today

        connection.Open()

        ' COLLECTION /////////////////////////////////////////////////////////////////////////////////////////////////////////////////

        Dim cmdIncidencia As New SqlCommand("SELECT I.IdIncidencia, I.IdExpediente, I.Pagado, I.Fecha FROM Incidencias I INNER JOIN Expedientes E ON I.IdExpediente = E.IdExpediente WHERE Fecha >= CAST(GETDATE() AS DATE) AND E.idCliente IN (84, 85, 86, 87, 88, 89, 90, 91) ORDER BY Fecha ASC", connection)
        'Dim cmdIncidencia As New SqlCommand("SELECT I.IdIncidencia, I.IdExpediente, I.Pagado, I.Fecha FROM Incidencias I INNER JOIN Expedientes E ON I.IdExpediente = E.IdExpediente WHERE Fecha >= CONVERT(DATETIME, '2024-04-03 00:00:00', 120) AND Fecha < CONVERT(DATETIME, '2024-04-04 00:00:00', 120) AND E.idCliente IN (84, 85, 86, 87, 88, 89, 90, 91) ORDER BY Fecha ASC", connection)
        Dim rIncidencias As SqlDataReader = cmdIncidencia.ExecuteReader()
        Dim idExpediente As String, codFactura As Object, codigoBarras As Object, fecha As Object, formatFecha As String, pagado As Object, anyo As String, mes As String, dia As String, hora As String

        Dim writerColl As New StreamWriter(IO.Path.Combine(path, "Daily_collection_report_MC2Legal_" & today & ".csv"))
        writerColl.WriteLine("Payment ID,InternalDebtID,OriginalAccountID,Timestamp,Amount")
        Dim cmdRecibos As New SqlCommand
        While rIncidencias.Read()
            idExpediente = rIncidencias("IdExpediente")

            cmdRecibos = New SqlCommand("SELECT CodFactura, codigobarras FROM Recibos WHERE IdExpediente = @idExpediente", connection)
            cmdRecibos.Parameters.Add("@idExpediente", SqlDbType.Int).Value = idExpediente

            Dim reader As SqlDataReader = cmdRecibos.ExecuteReader()
            If reader.Read Then
                codFactura = reader.GetString(0).Replace(",", "")
                If Not reader.IsDBNull(1) Then
                    codigoBarras = reader.GetString(1).Replace(",", "")
                Else
                    codigoBarras = ""
                End If
            Else
                codFactura = ""
                codigoBarras = ""
            End If

            reader.Close()

            fecha = If(rIncidencias("Fecha") Is Nothing, "", rIncidencias("Fecha").ToString.Replace("/", "-"))
            anyo = Mid(fecha, 7, 4)
            mes = Mid(fecha, 4, 2)
            dia = Mid(fecha, 1, 2)
            hora = Mid(fecha, 12, 8)

            formatFecha = anyo & "-" & mes & "-" & dia & " " & hora
            pagado = If(rIncidencias("Pagado") Is Nothing, "", formatPagado(rIncidencias("Pagado").ToString.Replace(",", ".")))

            writerColl.WriteLine(rIncidencias("IdIncidencia").ToString.Trim & "," & codigoBarras.ToString.Trim & "," & codFactura.ToString.Trim & "," & formatFecha & "," & pagado)
        End While
        rIncidencias.Close()
        writerColl.Close()
        Console.WriteLine("CSV 1 Collestions creado. Tiempo: " & stopwatch.Elapsed.ToString)

        ' STATUS /////////////////////////////////////////////////////////////////////////////////////////////////////////////////

        Dim cmdAcciones1 As New SqlCommand("SELECT A.IdAccion, A.IdExpediente FROM Acciones A JOIN Expedientes E ON A.IdExpediente = E.IdExpediente WHERE A.Fecha >= CAST(GETDATE() AS DATE) AND E.idCliente in (84,85,86,87,88,89,90,91) ORDER BY A.Fecha ASC", connection) 'WHERE A.Fecha >= CONVERT(DATETIME, '2024-03-26 00:00:00', 120) AND A.Fecha < CONVERT(DATETIME, '2024-03-27 00:00:00', 120) AND E.idCliente in (84,85,86,87,88,89,90,91) ORDER BY A.Fecha ASC", connection)
        'Dim cmdAcciones1 As New SqlCommand("SELECT A.IdAccion, A.IdExpediente FROM Acciones A JOIN Expedientes E ON A.IdExpediente = E.IdExpediente WHERE A.Fecha >= CONVERT(DATETIME, '2024-04-03 00:00:00', 120) AND Fecha < CONVERT(DATETIME, '2024-04-04 00:00:00', 120) AND E.idCliente in (84,85,86,87,88,89,90,91) ORDER BY A.Fecha ASC", connection)
        Dim rAcciones1 As SqlDataReader = cmdAcciones1.ExecuteReader()
        Dim idAccion As Object, descripcionNota As Object

        Dim writerStat As New StreamWriter(IO.Path.Combine(path, "Daily_account_status_report_MC2Legal_" & today & ".csv"))
        writerStat.WriteLine("InternalDebtID,OriginalAccountID,Account status")
        Dim cmdTipoNota1 As New SqlCommand
        While rAcciones1.Read()
            idExpediente = rAcciones1("IdExpediente")

            cmdRecibos = New SqlCommand("SELECT CodFactura, codigobarras FROM Recibos WHERE IdExpediente = @idExpediente", connection)
            cmdRecibos.Parameters.Add("@idExpediente", SqlDbType.Int).Value = idExpediente

            Dim reader As SqlDataReader = cmdRecibos.ExecuteReader()
            If reader.Read Then
                codFactura = reader.GetString(0)
                If Not reader.IsDBNull(1) Then
                    codigoBarras = reader.GetString(1).Replace(",", "")
                Else
                    codigoBarras = ""
                End If
            Else
                codFactura = ""
                codigoBarras = ""
            End If

            reader.Close()

            idAccion = rAcciones1("IdAccion")
            cmdTipoNota1 = New SqlCommand("SELECT Descripcion FROM ( SELECT t.Descripcion, a.IdAccion FROM TipoNota AS t INNER JOIN Acciones AS a ON t.idTipoNota = a.idTipoNota WHERE a.IdAccion=" & idAccion & ") AS T GROUP BY Descripcion", connection)
            descripcionNota = If(cmdTipoNota1.ExecuteScalar() Is Nothing, "", cmdTipoNota1.ExecuteScalar().ToString().Replace(",", ""))

            writerStat.WriteLine(codigoBarras.ToString.Trim & "," & codFactura.ToString.Trim & "," & descripcionNota.ToString.Trim)
        End While
        rAcciones1.Close()
        writerStat.Close()
        Console.WriteLine("CSV 2 Statuses creado. Tiempo: " & stopwatch.Elapsed.ToString)

        ' COMMUNICATION /////////////////////////////////////////////////////////////////////////////////////////////////////////////////

        Dim cmdAcciones2 As New SqlCommand("SELECT A.IdExpediente, TN.idTipoNota, A.Fecha, RL.IdTipoAccion FROM Acciones AS A JOIN Expedientes AS E ON A.IdExpediente = E.IdExpediente JOIN ResultadoLlamada AS RL ON A.IdAccion = RL.IdAccion JOIN TipoNota AS TN ON A.IdTipoNota = TN.idTipoNota WHERE A.Fecha >= CAST(GETDATE() AS DATE) AND E.idCliente IN (84, 85, 86, 87, 88, 89, 90, 91) AND RL.IdTipoAccion IN (5, 6, 7, 8, 9, 10, 13, 14, 15) AND A.idTipoNota IN (5, 7, 8, 9, 10, 11, 13, 14, 15, 22, 79, 80, 81, 656, 742, 743, 744, 746, 788, 789, 820, 821, 822, 823, 824, 825, 826, 827, 828, 831, 832, 834, 836, 837, 840, 849, 854, 861, 915, 917, 918, 921, 922) ORDER BY A.Fecha ASC;", connection)
        'Dim cmdAcciones2 As New SqlCommand("SELECT A.IdExpediente, TN.idTipoNota, A.Fecha, RL.IdTipoAccion FROM Acciones AS A JOIN Expedientes AS E ON A.IdExpediente = E.IdExpediente JOIN ResultadoLlamada AS RL ON A.IdAccion = RL.IdAccion JOIN TipoNota AS TN ON A.IdTipoNota = TN.idTipoNota WHERE A.Fecha >= CONVERT(DATETIME, '2024-04-03 00:00:00', 120) AND Fecha < CONVERT(DATETIME, '2024-04-04 00:00:00', 120) AND E.idCliente IN (84, 85, 86, 87, 88, 89, 90, 91) AND RL.IdTipoAccion IN (5, 6, 7, 8, 9, 10, 13, 14, 15) AND A.idTipoNota IN (5, 7, 8, 9, 10, 11, 13, 14, 15, 22, 79, 80, 81, 656, 742, 743, 744, 746, 788, 789, 820, 821, 822, 823, 824, 825, 826, 827, 828, 831, 832, 834, 836, 837, 840, 849, 854, 861, 915, 917, 918, 921, 922) ORDER BY A.Fecha ASC;", connection)
        Dim rAcciones2 As SqlDataReader = cmdAcciones2.ExecuteReader()
        Dim numTipoNota As Object, idTipoAccion As Object, descripcion As String

        Dim writerComm As New StreamWriter(IO.Path.Combine(path, "Daily_communication_report_MC2Legal_" & today & ".csv"))
        writerComm.WriteLine("OriginalAccountID,InternalDebtID,ChannelID,ExecutedAt,Direction")
        Dim cmdTipoNota2 As New SqlCommand

        fecha = ""
        formatFecha = ""
        While rAcciones2.Read()
            idExpediente = rAcciones2("IdExpediente")
            numTipoNota = If(rAcciones2("idTipoNota") Is Nothing, "", Convert.ToInt32(rAcciones2("idTipoNota")))
            fecha = If(rAcciones2("Fecha") Is Nothing, "", rAcciones2("Fecha").ToString.Replace("/", "-"))
            anyo = Mid(fecha, 7, 4)
            mes = Mid(fecha, 4, 2)
            dia = Mid(fecha, 1, 2)
            hora = Mid(fecha, 12, 8)
            formatFecha = anyo & "-" & mes & "-" & dia & " " & hora

            descripcion = tipoNota(numTipoNota)

            idTipoAccion = If(rAcciones2("IdTipoAccion") Is Nothing, "", tipoAccion(Convert.ToUInt32(rAcciones2("IdTipoAccion"))))

            cmdRecibos = New SqlCommand("SELECT CodFactura, codigobarras FROM Recibos WHERE IdExpediente = @idExpediente", connection)
            cmdRecibos.Parameters.Add("@idExpediente", SqlDbType.Int).Value = idExpediente

            Dim reader As SqlDataReader = cmdRecibos.ExecuteReader()
            If reader.Read Then
                codFactura = reader.GetString(0).Replace(",", "")
                If Not reader.IsDBNull(1) Then
                    codigoBarras = reader.GetString(1).Replace(",", "")
                Else
                    codigoBarras = ""
                End If
            Else
                codFactura = ""
                codigoBarras = ""
            End If

            reader.Close()

            writerComm.WriteLine(codFactura.ToString.Trim & "," & codigoBarras.ToString.Trim & "," & descripcion & "," & formatFecha & "," & idTipoAccion.ToString)
        End While
        rAcciones2.Close()
        writerComm.Close()
        Console.WriteLine("CSV 3 Communications creado. Tiempo: " & stopwatch.Elapsed.ToString)

        connection.Close()
        stopwatch.Stop()
    End Sub

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
            Case 22
                Return "Phone"
            Case 79 To 81
                Return "Letter"
            Case 656, 744, 746, 854
                Return "Letter"
            Case 742
                Return "Phone"
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

    Function tipoAccion(idTipoAccion As Integer) As String
        If (idTipoAccion <= 7) Then
            Return "Outbound"
        Else
            Return "Inbound"
        End If
    End Function

End Module