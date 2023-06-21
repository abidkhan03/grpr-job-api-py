# Generated by Django 3.2.4 on 2021-06-09 10:23

from django.db import migrations, models


class Migration(migrations.Migration):

    initial = True

    dependencies = [
    ]

    operations = [
        migrations.CreateModel(
            name='CombinedJob',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('created', models.DateTimeField(auto_now_add=True)),
                ('report_title', models.TextField()),
                ('grouping_method', models.CharField(blank=True, default='', max_length=50)),
                ('main_keyword_grouping_accuracy', models.IntegerField()),
                ('variant_keyword_grouping_accuracy', models.IntegerField()),
                ('country', models.CharField(blank=True, default='', max_length=100)),
                ('location', models.CharField(blank=True, default='', max_length=100)),
                ('language', models.CharField(blank=True, default='', max_length=100)),
            ],
            options={
                'ordering': ['created'],
            },
        ),
    ]
