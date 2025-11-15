from flask import Flask, render_template, request, redirect, session, url_for, flash, get_flashed_messages, make_response, send_file, jsonify
from flask_paginate import Pagination, get_page_args
from decimal import Decimal
from datetime import datetime, timedelta 
from collections import defaultdict
import psycopg2
from psycopg2 import Error, pool
from psycopg2.extras import RealDictCursor
import csv
import secrets

app = Flask(__name__)

app.secret_key = secrets.token_hex(32)

app.config['SESSION_COOKIE_SECURE'] = False
app.config['SESSION_COOKIE_HTTPONLY'] = True
app.config['SESSION_COOKIE_SAMESITE'] = 'Lax'
app.config['PERMANENT_SESSION_LIFETIME'] = timedelta(hours=24)

# Configuração BD PostgreSQL
db_config = {
    "host": "dpg-d4c8qn2dbo4c73d44pag-a.oregon-postgres.render.com",  # Ajuste conforme necessário
    "user": "sci_user",  # Substitua pelo seu usuário
    "password": "8eHUw4PrB33IkodiE1Ms0qoj73jPT1tK",  # Substitua pela sua senha
    "database": "sci",
    "port": 5432  # Porta padrão do PostgreSQL
}

# Pool de conexões para melhor performance
connection_pool = psycopg2.pool.SimpleConnectionPool(1, 20, **db_config)

def get_db_connection():
    """Obtém conexão do pool"""
    return connection_pool.getconn()

def release_db_connection(connection):
    """Devolve conexão ao pool"""
    connection_pool.putconn(connection)

# Página Home
@app.route('/')
def home():
    return render_template('index.html')

# Cadastro de empresas
@app.route('/cadastro-empresas', methods=['GET', 'POST'])
def cadastro_empresas():
    if request.method == 'POST':
        codigo_empresa = request.form['codigo_empresa']
        razao_social = request.form['razao_social']
        valor_diaria = Decimal(request.form['valor_diaria'])

        connection = None
        try:
            connection = get_db_connection()
            cursor = connection.cursor()
            sql = "INSERT INTO empresas (codigo_empresa, razao_social, valor_diaria) VALUES (%s, %s, %s)"
            val = (codigo_empresa, razao_social, valor_diaria)
            cursor.execute(sql, val)
            connection.commit()
            cursor.close()
            flash('Empresa Cadastrada!', 'success')
            return redirect(url_for('cadastro_empresas'))
        except Error as e:
            if connection:
                connection.rollback()
            return f'Error: {e}'
        finally:
            if connection:
                release_db_connection(connection)

    # Consultar empresas cadastradas
    def consultar_empresas():
        connection = None
        try:
            connection = get_db_connection()
            cursor = connection.cursor()
            sql = "SELECT * FROM empresas ORDER BY codigo_empresa"
            cursor.execute(sql)
            empresas = cursor.fetchall()
            cursor.close()
            empresas_dict = []
            for empresa in empresas:
                empresas_dict.append({
                    'codigo_empresa': empresa[1],
                    'razao_social': empresa[2],
                    'valor_diaria': empresa[3]
                })
            return empresas_dict
        except Error as e:
            return f'Error: {e}'
        finally:
            if connection:
                release_db_connection(connection)

    empresas = consultar_empresas()
    return render_template('cadastro_empresas.html', empresas=empresas)


# Cadastro de empregados
@app.route('/cadastro-empregados', methods=['GET', 'POST'])
def cadastro_empregados():
    if request.method == 'POST':
        codigo = request.form['codigo']
        nome_completo = request.form['nome_completo']
        cargo = request.form['cargo']
        data_admissao = request.form['data_admissao']

        # Verificar se já tem funcionário com o código 
        if check_funcionario_exists(codigo):
            flash('Já existe funcionário com o código informado!', 'danger')
            return redirect(url_for('cadastro_empregados'))

        try:
            create_funcionario(codigo, nome_completo, cargo, data_admissao)
            flash('Empregado cadastrado!', 'success')
            return redirect(url_for('cadastro_empregados'))
        except Error as e:
            flash(f'Error: {e}', 'danger')
            return redirect(url_for('cadastro_empregados'))

    empresas = get_empresas()
    funcionarios = consulta_empregados()
    return render_template('cadastro-empregados.html', empresas=empresas, funcionarios=funcionarios)

def check_funcionario_exists(codigo):
    connection = None
    try:
        connection = get_db_connection()
        cursor = connection.cursor()
        sql = "SELECT 1 FROM funcionarios WHERE codigo = %s"
        val = (codigo,)
        cursor.execute(sql, val)
        result = cursor.fetchone() is not None
        cursor.close()
        return result
    except Error as e:
        flash(f'Error: {e}', 'danger')
        return False
    finally:
        if connection:
            release_db_connection(connection)

def create_funcionario(codigo, nome_completo, cargo, data_admissao):
    connection = None
    try:
        connection = get_db_connection()
        cursor = connection.cursor()
        sql = "INSERT INTO funcionarios (codigo, nome_completo, cargo, data_admissao) VALUES (%s, %s, %s, %s)"
        val = (codigo, nome_completo, cargo, data_admissao)
        cursor.execute(sql, val)
        connection.commit()
        cursor.close()
    except Error as e:
        if connection:
            connection.rollback()
        raise e
    finally:
        if connection:
            release_db_connection(connection)

def get_empresas():
    connection = None
    try:
        connection = get_db_connection()
        cursor = connection.cursor()
        sql = "SELECT razao_social FROM empresas ORDER BY razao_social"
        cursor.execute(sql)
        result = cursor.fetchall()
        cursor.close()
        return result
    except Error as e:
        flash(f'Error: {e}', 'danger')
        return []
    finally:
        if connection:
            release_db_connection(connection)

def consulta_empregados():
    connection = None
    try:
        connection = get_db_connection()
        cursor = connection.cursor()
        sql = "SELECT * FROM funcionarios ORDER BY codigo"
        cursor.execute(sql)
        funcionarios = cursor.fetchall()
        cursor.close()
        funcionarios_dict = []
        for funcionario in funcionarios:
            funcionarios_dict.append({
                'codigo': funcionario[1],
                'nome_completo': funcionario[2],
                'cargo': funcionario[3],
                'data_admissao': funcionario[4]
            })
        return funcionarios_dict
    except Error as e:
        flash(f'Error retrieving employees: {e}', 'danger')
        return []
    finally:
        if connection:
            release_db_connection(connection)

@app.route('/editar-funcionario/<int:funcionario_codigo>', methods=['GET', 'POST'])
def editar_funcionario(funcionario_codigo):
    if request.method == 'POST':
        nome_completo = request.form['nome_completo']
        cargo = request.form['cargo']
        data_admissao = request.form['data_admissao']

        try:
            update_funcionario(funcionario_codigo, nome_completo, cargo, data_admissao)
            flash('Funcionário editado com sucesso!', 'success')
            return redirect(url_for('cadastro_empregados'))
        except Error as e:
            flash(f'Error: {e}', 'danger')
            return redirect(url_for('editar_funcionario', funcionario_codigo=funcionario_codigo))

    connection = None
    try:
        connection = get_db_connection()
        cursor = connection.cursor()
        sql = "SELECT * FROM funcionarios WHERE codigo = %s"
        val = (funcionario_codigo,)
        cursor.execute(sql, val)
        funcionario = cursor.fetchone()
        cursor.close()
    except Error as e:
        flash(f'Error: {e}', 'danger')
        return redirect(url_for('cadastro_empregados'))
    finally:
        if connection:
            release_db_connection(connection)

    return render_template('editar-funcionario.html', funcionario=funcionario, funcionario_codigo=funcionario_codigo)

def update_funcionario(funcionario_codigo, nome_completo, cargo, data_admissao):
    connection = None
    try:
        connection = get_db_connection()
        cursor = connection.cursor()
        sql = "UPDATE funcionarios SET nome_completo = %s, cargo = %s, data_admissao = %s WHERE codigo = %s"
        val = (nome_completo, cargo, data_admissao, funcionario_codigo)
        cursor.execute(sql, val)
        connection.commit()
        cursor.close()
        return True
    except Error as e:
        if connection:
            connection.rollback()
        print(f"Error: {e}")
        return False
    finally:
        if connection:
            release_db_connection(connection)

# Cadastro de tarefas
TAREFA_ESCOLHAS = [('CORTE', 'CORTE'), ('DIARIA', 'DIARIA'), ('FALTA', 'FALTA'), ('CHUVA', 'CHUVA'), ('ATESTADO', 'ATESTADO'), ('DESCONTO ALIMENTAÇÃO', 'DESCONTO ALIMENTAÇÃO'), ('HORAS EXTRAS', 'HORAS EXTRAS')]

@app.route('/cadastro_tarefas', methods=['GET', 'POST'])    
def cadastro_tarefas():
    tarefas = consulta_tarefas()
    if request.method == 'POST':
        data = request.form['data']
        funcionario_codigo = request.form['funcionario']
        tarefa = request.form['tarefa']
        quantidade = Decimal(request.form['quantidade'])
        valor = Decimal(request.form['valor'])
        total = quantidade * valor

        connection = None
        try:
            connection = get_db_connection()
            cursor = connection.cursor()
            sql = "SELECT nome_completo FROM funcionarios WHERE codigo = %s"
            val = (funcionario_codigo,)
            cursor.execute(sql, val)
            result = cursor.fetchone()
            funcionario_nome_completo = result[0] if result else None
            cursor.close()
        except Error as e:
            if connection:
                release_db_connection(connection)
            return f'Error: {e}'

        try:
            cursor = connection.cursor()
            sql = "INSERT INTO tarefas (data, funcionario, tarefa, quantidade, valor, total) VALUES (%s, %s, %s, %s, %s, %s)"
            val = (data, funcionario_codigo, tarefa, quantidade, valor, total)
            cursor.execute(sql, val)
            connection.commit()
            cursor.close()
            message = "Tarefa cadastrada!"
            tarefas = consulta_tarefas()
            return render_template('cadastro_tarefas.html', message=message, data=data, nome_completo=funcionario_nome_completo, tarefas=tarefas, tarefa_selecionada=tarefa, valor=valor)
        except Error as e:
            if connection:
                connection.rollback()
            return f'Error: {e}'
        finally:
            if connection:
                release_db_connection(connection)
    
    tarefas = consulta_tarefas()
    return render_template('cadastro_tarefas.html', tarefas=tarefas, valor=request.form.get('valor', ''), tarefa_selecionada=request.form.get('tarefa', ''))

def consulta_tarefas():
    connection = None
    try:
        connection = get_db_connection()
        cursor = connection.cursor()
        sql = "SELECT * FROM tarefas ORDER BY id DESC LIMIT 30"
        cursor.execute(sql)
        tarefas = cursor.fetchall()
        cursor.close()
        tarefas_dict = []
        for tarefa in tarefas:
            tarefas_dict.append({
                'id': tarefa[0],
                'data': tarefa[1],
                'funcionario': tarefa[2],
                'tarefa': tarefa[3],
                'quantidade': tarefa[4],
                'valor': tarefa[5],
                'total': tarefa[6]
            })
        return tarefas_dict
    except Error as e:
        print(f'Error: {e}')
        return []
    finally:
        if connection:
            release_db_connection(connection)

@app.route('/delete_tarefa/<int:tarefa_id>')
def delete_tarefa_route(tarefa_id):
    connection = None
    try:
        connection = get_db_connection()
        cursor = connection.cursor()
        sql = "DELETE FROM tarefas WHERE id = %s"
        val = (tarefa_id,)
        cursor.execute(sql, val)
        connection.commit()
        cursor.close()
        flash("TAREFA EXCLUÍDA")
    except Error as e:
        if connection:
            connection.rollback()
        print(f'Error: {e}')
        return f'Error: {e}'
    finally:
        if connection:
            release_db_connection(connection)
    return redirect(url_for('cadastro_tarefas'))

@app.route('/get-funcionario-nome-completo/<int:funcionario_codigo>')
def get_funcionario_nome_completo(funcionario_codigo):
    connection = None
    try:
        connection = get_db_connection()
        cursor = connection.cursor()
        sql = "SELECT nome_completo FROM funcionarios WHERE codigo = %s"
        val = (funcionario_codigo,)
        cursor.execute(sql, val)
        result = cursor.fetchone()
        cursor.close()
        if result:
            nome_completo = result[0]
            return jsonify({'nome_completo': nome_completo})
        else:
            return jsonify({'error': 'Digite um código válido'}), 404
    except Error as e:
        return jsonify({'error': f'Error: {e}'}), 500
    finally:
        if connection:
            release_db_connection(connection)

@app.route('/get_tarefas/<int:funcionario_codigo>')
def get_tarefas(funcionario_codigo):
    connection = None
    try:
        connection = get_db_connection()
        cursor = connection.cursor()
        sql = "SELECT data, tarefa, quantidade, valor, total FROM tarefas WHERE funcionario = %s ORDER BY data DESC LIMIT 30"
        val = (funcionario_codigo,)
        cursor.execute(sql, val)
        tarefas = cursor.fetchall()
        cursor.close()
        return jsonify(tarefas)
    except Error as e:
        return f'Error: {e}'
    finally:
        if connection:
            release_db_connection(connection)

# Consultar e excluir tarefas
@app.route('/consulta_tarefas_por_periodo', methods=['GET', 'POST'])
@app.route('/consulta_tarefas_por_periodo/<string:data_inicial>/<string:data_final>/<int:funcionario_codigo>', methods=['GET', 'POST'])
def consulta_tarefas_por_periodo(data_inicial=None, data_final=None, funcionario_codigo=None):
    if request.method == 'POST':
        data_inicial = request.form['data_inicial']
        data_final = request.form['data_final']
        funcionario_codigo = request.form['funcionario']      

        connection = None
        try:
            connection = get_db_connection()
            cursor = connection.cursor()
            sql = """
                SELECT * FROM tarefas
                WHERE data BETWEEN %s AND %s AND funcionario = %s
                ORDER BY data DESC
            """
            val = (data_inicial, data_final, funcionario_codigo)
            cursor.execute(sql, val)
            tarefas = cursor.fetchall()
            cursor.close()

            tarefas_dict = []
            for tarefa in tarefas:
                tarefas_dict.append({
                    'id': tarefa[0],
                    'data': tarefa[1],
                    'funcionario': tarefa[2],
                    'tarefa': tarefa[3],
                    'quantidade': tarefa[4],
                    'valor': tarefa[5],
                    'total': tarefa[6]
                })
            if not tarefas_dict:
                flash("SEM TAREFAS LANÇADAS NESTE PERÍODO PARA ESTE FUNCIONÁRIO")

            flash_messages = [message for message in get_flashed_messages()]

            return render_template('consulta_tarefas_por_periodo.html', tarefas=tarefas_dict, data_inicial=data_inicial, data_final=data_final, funcionario_codigo=funcionario_codigo, flash_messages=flash_messages)

        except Error as e:
            return f'Error: {e}'
        finally:
            if connection:
                release_db_connection(connection)

    elif data_inicial and data_final and funcionario_codigo:
        connection = None
        try:
            connection = get_db_connection()
            cursor = connection.cursor()
            sql = """
                SELECT * FROM tarefas
                WHERE data BETWEEN %s AND %s AND funcionario = %s
                ORDER BY data DESC
            """
            val = (data_inicial, data_final, funcionario_codigo)
            cursor.execute(sql, val)
            tarefas = cursor.fetchall()
            cursor.close()

            tarefas_dict = []
            for tarefa in tarefas:
                tarefas_dict.append({
                    'id': tarefa[0],
                    'data': tarefa[1],
                    'funcionario': tarefa[2],
                    'tarefa': tarefa[3],
                    'quantidade': tarefa[4],
                    'valor': tarefa[5],
                    'total': tarefa[6]
                })
            if not tarefas_dict:
                flash("SEM TAREFAS LANÇADAS NESTE PERÍODO PARA ESTE FUNCIONÁRIO")

            flash_messages = [message for message in get_flashed_messages()]

            return render_template('consulta_tarefas_por_periodo.html', tarefas=tarefas_dict, data_inicial=data_inicial, data_final=data_final, funcionario_codigo=funcionario_codigo, flash_messages=flash_messages)

        except Error as e:
            return f'Error: {e}'
        finally:
            if connection:
                release_db_connection(connection)

    return render_template('consulta_tarefas_por_periodo.html')

@app.route('/excluir_tarefa/<int:tarefa_id>/<string:data_inicial>/<string:data_final>/<int:funcionario_codigo>', methods=['GET', 'POST'])
def excluir_tarefa(tarefa_id, data_inicial, data_final, funcionario_codigo):
    connection = None
    try:
        connection = get_db_connection()
        cursor = connection.cursor()
        sql = "DELETE FROM tarefas WHERE id = %s"
        val = (tarefa_id,)
        cursor.execute(sql, val)
        connection.commit()
        cursor.close()
        flash("TAREFA EXCLUÍDA")
        return redirect(url_for('consulta_tarefas_por_periodo', data_inicial=data_inicial, data_final=data_final, funcionario_codigo=funcionario_codigo))
    except Error as e:
        if connection:
            connection.rollback()
        return f'Error: {e}'
    finally:
        if connection:
            release_db_connection(connection)
    
@app.route('/editar_tarefa/<int:tarefa_id>/<string:data_inicial>/<string:data_final>/<int:funcionario_codigo>', methods=['GET', 'POST'])
def editar_tarefa(tarefa_id, data_inicial, data_final, funcionario_codigo):
    if request.method == 'POST':
        tarefa = request.form['tarefa']
        quantidade = request.form['quantidade']
        valor = request.form['valor']
        total = float(quantidade) * float(valor)

        connection = None
        try:
            connection = get_db_connection()
            cursor = connection.cursor()
            sql = """
                UPDATE tarefas
                SET tarefa = %s, quantidade = %s, valor = %s, total = %s
                WHERE id = %s
            """
            val = (tarefa, quantidade, valor, total, tarefa_id)
            cursor.execute(sql, val)
            connection.commit()
            cursor.close()
            flash("TAREFA EDITADA")
            return redirect(url_for('consulta_tarefas_por_periodo', data_inicial=data_inicial, data_final=data_final, funcionario_codigo=funcionario_codigo))
        except Error as e:
            if connection:
                connection.rollback()
            return f'Error: {e}'
        finally:
            if connection:
                release_db_connection(connection)

    connection = None
    try:
        connection = get_db_connection()
        cursor = connection.cursor()
        sql = "SELECT * FROM tarefas WHERE id = %s"
        val = (tarefa_id,)
        cursor.execute(sql, val)
        tarefa = cursor.fetchone()
        cursor.close()

        tarefa_dict = {
            'id': tarefa[0],
            'data': tarefa[1],
            'funcionario': tarefa[2],
            'tarefa': tarefa[3],
            'quantidade': tarefa[4],
            'valor': tarefa[5],
            'total': tarefa[6]
        }

        return render_template('editar_tarefa.html', tarefa=tarefa_dict, data_inicial=data_inicial, data_final=data_final, funcionario_codigo=funcionario_codigo)

    except Error as e:
        return f'Error: {e}'
    finally:
        if connection:
            release_db_connection(connection)
    
# Complementar mínima diária    
@app.route('/complementar_tarefas_periodo', methods=['GET', 'POST'])
def complementar_tarefas_periodo():
    if request.method == 'POST':
        data_inicial = request.form['data_inicial']
        data_final = request.form['data_final']
        connection = None
        try:
            connection = get_db_connection()
            cursor = connection.cursor()
            
            # Excluir COMPLEMENTO existentes no período
            sql_delete = "DELETE FROM tarefas WHERE data BETWEEN %s AND %s AND tarefa = 'COMPLEMENTO'"
            val_delete = (data_inicial, data_final)
            cursor.execute(sql_delete, val_delete)
            connection.commit()

            # Inserir novos COMPLEMENTO
            sql = "SELECT data, funcionario, SUM(total) as total FROM tarefas WHERE tarefa = 'CORTE' AND data BETWEEN %s AND %s GROUP BY data, funcionario HAVING SUM(total) < 54.67"
            val = (data_inicial, data_final)
            cursor.execute(sql, val)
            tarefas = cursor.fetchall()
            
            for tarefa in tarefas:
                data = tarefa[0]
                funcionario = tarefa[1]
                total_faltante = Decimal('54.67') - tarefa[2]

                sql = "INSERT INTO tarefas (data, funcionario, tarefa, quantidade, valor, total) VALUES (%s, %s, 'COMPLEMENTO', %s, %s, %s)"
                val = (data, funcionario, 1, total_faltante, total_faltante)
                cursor.execute(sql, val)
            
            connection.commit()
            cursor.close()
            flash('Tarefas complementadas com sucesso!', 'success')
        except Error as e:
            if connection:
                connection.rollback()
            flash(f'Error: {e}', 'danger')
        finally:
            if connection:
                release_db_connection(connection)

        return redirect(url_for('complementar_tarefas_periodo'))
    return render_template('complementar_tarefas_periodo.html')


# GERAR DESCONTO DSR
@app.route('/complementar_desconto_dsr', methods=['GET', 'POST'])
def complementar_desconto_dsr():
    if request.method == 'POST':
        data_inicial = request.form['data_inicial']
        data_final = request.form['data_final']
        connection = None
        try:
            connection = get_db_connection()
            cursor = connection.cursor()
            
            # Encontrar o último dia da semana (sexta-feira) para cada semana dentro do período especificado
            # No PostgreSQL usamos EXTRACT(WEEK FROM data) ao invés de WEEK(data)
            sql = """
                SELECT DISTINCT EXTRACT(WEEK FROM data) AS semana, 
                                MAX(data) AS ultima_sexta
                FROM tarefas
                WHERE data BETWEEN %s AND %s
                GROUP BY EXTRACT(WEEK FROM data)
            """
            val = (data_inicial, data_final)
            cursor.execute(sql, val)
            ultimas_sextas = cursor.fetchall()

            for semana, ultima_sexta in ultimas_sextas:
                # Verificar se há pelo menos uma falta para qualquer funcionário dentro dessa semana
                sql = """
                    SELECT COUNT(*) AS faltas_semanais
                    FROM tarefas
                    WHERE tarefa = 'FALTA' AND EXTRACT(WEEK FROM data) = %s
                """
                val = (semana,)
                cursor.execute(sql, val)
                resultado = cursor.fetchone()

                if resultado[0] > 0:
                    sql = """
                        SELECT COUNT(*) AS desconto_dsr_existente
                        FROM tarefas
                        WHERE tarefa = 'DESCONTO DSR' AND data = %s
                    """
                    val = (ultima_sexta,)
                    cursor.execute(sql, val)
                    resultado_desconto_dsr = cursor.fetchone()

                    if resultado_desconto_dsr[0] == 0:
                        # Inserir uma tarefa "DESCONTO DSR" para cada funcionário na última sexta-feira da semana
                        sql = """
                            INSERT INTO tarefas (data, funcionario, tarefa, quantidade, valor, total)
                            SELECT %s, funcionario, 'DESCONTO DSR', 1, 0, 0
                            FROM (SELECT DISTINCT funcionario FROM tarefas WHERE EXTRACT(WEEK FROM data) = %s) AS funcionarios_semana
                        """
                        val = (ultima_sexta, semana)
                        cursor.execute(sql, val)
            
            connection.commit()
            cursor.close()
            mensagem = "Concluído"
        except Error as e:
            if connection:
                connection.rollback()
            print(f'Error: {e}')
            return redirect(url_for('complementar_desconto_dsr'))
        finally:
            if connection:
                release_db_connection(connection)

        return render_template('complementar_desconto_dsr.html', mensagem=mensagem)
    else:
        return render_template('complementar_desconto_dsr.html')
    
# GERAR RELATÓRIO 
@app.route('/relatorio', methods=['GET', 'POST'])
def relatorio():
    if request.method == 'POST':
        data_inicial = request.form['data_inicial']
        data_final = request.form['data_final']
        return redirect(url_for('relatorio', data_inicial=data_inicial, data_final=data_final))
    else:
        data_inicial = request.args.get('data_inicial')
        data_final = request.args.get('data_final')

        if data_inicial and data_final:
            data_inicial = datetime.strptime(data_inicial, '%Y-%m-%d')
            data_final = datetime.strptime(data_final, '%Y-%m-%d')

            relatorio = gerar_relatorio(data_inicial, data_final)

            return render_template('relatorio.html', relatorio=relatorio, data_inicial=data_inicial, data_final=data_final)
        else:
            relatorio = None

        return render_template('relatorio.html', relatorio=relatorio, data_inicial=data_inicial, data_final=data_final)

def gerar_relatorio(data_inicial, data_final):
    connection = None
    try:
        connection = get_db_connection()
        cursor = connection.cursor()
        sql = """
            SELECT f.codigo, f.nome_completo, t.data, t.tarefa, t.quantidade, t.valor, t.total
            FROM funcionarios f
            JOIN tarefas t ON f.codigo = t.funcionario
            WHERE t.data BETWEEN %s AND %s
            ORDER BY f.codigo, t.data
        """
        val = (data_inicial, data_final)
        cursor.execute(sql, val)
        relatorio_data = cursor.fetchall()
        cursor.close()
    except Error as e:
        return f'Error: {e}'
    finally:
        if connection:
            release_db_connection(connection)

    funcionarios_data = defaultdict(list)
    for row in relatorio_data:
        funcionarios_data[row[0]].append({
            'nome_completo': row[1],
            'data': row[2],
            'tarefa': row[3],
            'quantidade': row[4],
            'valor': row[5],
            'total': row[6]
        })
        
    for funcionario_codigo, funcionario_tarefas in funcionarios_data.items():
        total_geral = sum(tarefa['total'] for tarefa in funcionario_tarefas)
        funcionario_tarefas.append({'total_geral': total_geral})    

    return funcionarios_data

@app.route('/relatorio_csv', methods=['GET', 'POST'])
def relatorio_csv():
    if request.method == 'POST':
        data_inicio = request.form['data_inicio']
        data_fim = request.form['data_fim']
        filename = request.form['filename']
        
        connection = None
        try:
            data_fim_datetime = datetime.strptime(data_fim, '%Y-%m-%d')
            competencia = data_fim_datetime.strftime('%Y%m')  # Formato AAAAMM
            
            connection = get_db_connection()
            cursor = connection.cursor()
            sql = """
                SELECT tarefas.funcionario, SUM(tarefas.total) AS total, 
                       COUNT(CASE WHEN tarefas.tarefa = 'FALTA' THEN 1 ELSE NULL END) AS falta_count
                FROM tarefas
                WHERE tarefas.data BETWEEN %s AND %s
                GROUP BY tarefas.funcionario
            """
            val = (data_inicio, data_fim)
            cursor.execute(sql, val)
            relatorio_data = cursor.fetchall()

            # Consulta para obter as datas das faltas
            sql_faltas = """
                SELECT tarefas.funcionario, tarefas.data
                FROM tarefas
                WHERE tarefas.tarefa = 'FALTA' AND tarefas.data BETWEEN %s AND %s
            """
            cursor.execute(sql_faltas, val)
            faltas_data = cursor.fetchall()
            cursor.close()

            # Cria arquivo TXT
            with open('/home/escritoriomartins/apps_wsgi/sici_reginaldo/' + filename, 'w') as txtfile:
                for row in relatorio_data:
                    funcionario = str(row[0]).zfill(10)  # Código do empregado (10 caracteres)
                    total = int(row[1] * 100)  # Multiplica por 100 para remover a casa decimal
                    total_str = str(total).zfill(9)  # Preenche com zeros à esquerda para ter 9 caracteres
                    codigo_rubrica = '000000523'  # Código da rubrica (não utilizado na saída)
                    tipo_processo = '11'  # Tipo do processo (fixo)
                    empresa = '1044'.zfill(10)  # Empresa (10 caracteres, fixo)

                    # Formata a linha de acordo com as especificações
                    linha = (
                        '10' +  # Posição 1-2: Fixo "10"
                        funcionario +  # Posição 3-12: Código do empregado
                        competencia +  # Posição 13-18: Competência (AAAAMM)
                        codigo_rubrica +
                        tipo_processo +  # Posição 19-20: Fixo "11"
                        total_str +  # Posição 21-29: Valor
                        empresa  # Posição 30-39: Empresa
                    )
                    
                    # Escreve a linha no arquivo
                    txtfile.write(linha + '\n')

                    # Adiciona nova linha para total de faltas se houver faltas
                    falta_count = row[2]  # Total de faltas
                    if falta_count > 0:
                        falta_count_str = str(falta_count).zfill(9)  # Preenche com zeros à esquerda para ter 9 caracteres
                        linha_faltas = (
                            '10' +  # Posição 1-2: Fixo "10"
                            funcionario +  # Posição 3-12: Código do empregado
                            competencia +  # Posição 13-18: Competência (AAAAMM)
                            '000000520' +  # Posição 19-27: Fixo "000000520"
                            tipo_processo +  # Posição 28-29: Fixo "11"
                            falta_count_str +  # Posição 30-38: Total de faltas
                            empresa  # Posição 39-48: Empresa
                        )

                        # Escreve a linha de faltas no arquivo
                        txtfile.write(linha_faltas + '\n')

                        # Adiciona novas linhas com as datas das faltas
                        for falta in faltas_data:
                            funcionario_falta = str(falta[0]).zfill(10)  # Código do empregado
                            data_falta = falta[1].strftime('%y%m%d')  # Formata a linha de data da falta
                            linha_data_falta = (
                                '11' +  # Posição 1-2: Fixo "11"
                                data_falta +  # Posição 3-10: Data da falta (aaammdd)
                                '1'  # Posição 11: Fixo "1"
                            )

                            # Escreve a linha de data da falta no arquivo
                            txtfile.write(linha_data_falta + '\n')

            flash('Relatório gerado com sucesso!', 'success')

            # Send the file as a response
            return send_file(filename, as_attachment=True)

        except Error as e:
            if connection:
                connection.rollback()
            return f'Error: {e}'
        finally:
            if connection:
                release_db_connection(connection)

    return render_template('relatorio_csv.html')

if __name__ == '__main__':
    app.run(debug=True)